const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const net = std.net;
const EPOLL = linux.EPOLL;

const log = std.log.scoped(.event_loop);

pub const MAX_EPOLL_EVENTS = 128;
pub const TCP_BACKLOG = 128;

pub const FdType = enum {
    listener,
    signal,
    client,
    pipe,
    master_connecting,
};

pub const FdInfo = struct {
    fd: posix.fd_t,
    fd_type: FdType,
};

pub const Interest = struct {
    readable: bool = true,
    writable: bool = false,
};

pub const Event = union(enum) {
    accept: void,
    shutdown: void,
    client_io: ClientIoEvent,
    pipe_readable: posix.fd_t,
    master_connected: posix.fd_t,
};

pub const ClientIoEvent = struct {
    fd: posix.fd_t,
    readable: bool,
    writable: bool,
    hup: bool,
    err: bool,
};

pub const EventLoop = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    epoll_fd: posix.fd_t,
    listener_fd: posix.fd_t,

    fd_infos: std.AutoHashMap(posix.fd_t, *FdInfo),

    ready_list: [MAX_EPOLL_EVENTS]linux.epoll_event = undefined,

    pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !EventLoop {
        var sigmask: linux.sigset_t = .{0} ** 32;
        linux.sigaddset(&sigmask, linux.SIG.INT);
        linux.sigaddset(&sigmask, linux.SIG.TERM);
        _ = linux.sigprocmask(linux.SIG.BLOCK, &sigmask, null);

        const quit_fd: posix.fd_t = @intCast(linux.signalfd(-1, &sigmask, linux.SFD.CLOEXEC | linux.SFD.NONBLOCK));
        if (quit_fd < 0) return error.SignalFdFailed;
        errdefer posix.close(quit_fd);

        const address = try net.Address.resolveIp(host, port);

        const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
        const protocol = posix.IPPROTO.TCP;
        const listener_fd = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(listener_fd);

        try posix.setsockopt(listener_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        try posix.bind(listener_fd, &address.any, address.getOsSockLen());
        try posix.listen(listener_fd, TCP_BACKLOG);

        const epoll_fd = try posix.epoll_create1(0);
        errdefer posix.close(epoll_fd);

        var fd_infos = std.AutoHashMap(posix.fd_t, *FdInfo).init(allocator);
        errdefer fd_infos.deinit();

        var self = EventLoop{
            .allocator = allocator,
            .epoll_fd = epoll_fd,
            .listener_fd = listener_fd,
            .fd_infos = fd_infos,
        };

        try self.registerInternal(listener_fd, .listener, EPOLL.IN);
        try self.registerInternal(quit_fd, .signal, EPOLL.IN);

        log.info("EventLoop listening on {s}:{d}", .{ host, port });

        return self;
    }

    pub fn deinit(self: *Self) void {
        var it = self.fd_infos.iterator();
        while (it.next()) |entry| {
            posix.close(entry.value_ptr.*.fd);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.fd_infos.deinit();
        posix.close(self.epoll_fd);
        log.info("EventLoop shut down", .{});
    }

    pub fn acceptClient(self: *Self) !posix.fd_t {
        return try posix.accept(self.listener_fd, null, null, posix.SOCK.NONBLOCK);
    }

    pub fn registerFd(self: *Self, fd: posix.fd_t, fd_type: FdType, interest: Interest) !void {
        var events: u32 = 0;
        if (interest.readable) events |= EPOLL.IN;
        if (interest.writable) events |= EPOLL.OUT;
        try self.registerInternal(fd, fd_type, events);
    }

    pub fn promoteMasterToClient(self: *Self, fd: posix.fd_t) void {
        if (self.fd_infos.get(fd)) |info_ptr| {
            info_ptr.fd_type = .client;
        }
    }

    pub fn unregisterFd(self: *Self, fd: posix.fd_t) void {
        if (self.fd_infos.fetchRemove(fd)) |entry| {
            self.allocator.destroy(entry.value);
        }
    }

    pub fn modifyInterest(self: *Self, fd: posix.fd_t, interest: Interest) !void {
        const info_ptr = self.fd_infos.get(fd) orelse return error.FdNotRegistered;

        var events: u32 = 0;
        if (interest.readable) events |= EPOLL.IN;
        if (interest.writable) events |= EPOLL.OUT;

        var event = linux.epoll_event{
            .events = events,
            .data = .{ .ptr = @intFromPtr(info_ptr) },
        };
        const rc = linux.epoll_ctl(@intCast(self.epoll_fd), linux.EPOLL.CTL_MOD, @intCast(fd), &event);
        if (rc != 0) return error.EpollCtlFailed;
    }

    pub fn wait(self: *Self, timeout_ms: c_int) EventIterator {
        const events_ready = linux.epoll_wait(@intCast(self.epoll_fd), &self.ready_list, MAX_EPOLL_EVENTS, timeout_ms);
        const count: usize = if (events_ready > MAX_EPOLL_EVENTS) 0 else events_ready;
        return EventIterator{
            .ready_list = self.ready_list[0..count],
            .index = 0,
        };
    }

    fn registerInternal(self: *Self, fd: posix.fd_t, fd_type: FdType, events: u32) !void {
        const info_ptr = try self.allocator.create(FdInfo);
        errdefer self.allocator.destroy(info_ptr);

        info_ptr.* = .{ .fd = fd, .fd_type = fd_type };

        var event = linux.epoll_event{
            .events = events,
            .data = .{ .ptr = @intFromPtr(info_ptr) },
        };
        const rc = linux.epoll_ctl(@intCast(self.epoll_fd), linux.EPOLL.CTL_ADD, @intCast(fd), &event);
        if (rc != 0) {
            self.allocator.destroy(info_ptr);
            return error.EpollCtlFailed;
        }

        try self.fd_infos.put(fd, info_ptr);
    }
};

pub const EventIterator = struct {
    ready_list: []linux.epoll_event,
    index: usize,

    pub fn next(self: *EventIterator) ?Event {
        if (self.index >= self.ready_list.len) return null;

        const epoll_event = self.ready_list[self.index];
        self.index += 1;

        const info: *FdInfo = @ptrFromInt(epoll_event.data.ptr);
        const events = epoll_event.events;

        return switch (info.fd_type) {
            .listener => Event{ .accept = {} },
            .signal => Event{ .shutdown = {} },
            .pipe => Event{ .pipe_readable = info.fd },
            .master_connecting => Event{ .master_connected = info.fd },
            .client => Event{ .client_io = .{
                .fd = info.fd,
                .readable = (events & EPOLL.IN) != 0,
                .writable = (events & EPOLL.OUT) != 0,
                .hup = (events & EPOLL.HUP) != 0,
                .err = (events & EPOLL.ERR) != 0,
            } },
        };
    }
};
