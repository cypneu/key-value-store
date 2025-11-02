const std = @import("std");
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;
const EPOLL = linux.EPOLL;
const WriteOp = @import("state.zig").WriteOp;

const log = std.log.scoped(.server);

const MAX_EPOLL_EVENTS = 128;
const TCP_BACKLOG = 128;
const READ_BUFFER_SIZE = 16 * 1024;

pub fn Server(comptime H: type) type {
    return struct {
        const Self = @This();
        const FlushResult = enum { Idle, Pending, Closed };

        const PendingWriteState = struct {
            buffer: std.ArrayList(u8),
            written: usize,
            write_interest: bool,

            fn init(allocator: std.mem.Allocator) PendingWriteState {
                return .{
                    .buffer = std.ArrayList(u8).init(allocator),
                    .written = 0,
                    .write_interest = false,
                };
            }

            fn deinit(self: *PendingWriteState) void {
                self.buffer.deinit();
                self.written = 0;
                self.write_interest = false;
            }

            fn hasPendingData(self: PendingWriteState) bool {
                return self.written < self.buffer.items.len;
            }
        };

        handler: *H,
        allocator: std.mem.Allocator,
        epoll_fd: posix.fd_t,
        listener_fd: posix.fd_t,
        quit_fd: posix.fd_t,
        pending_writes: std.AutoHashMap(posix.fd_t, PendingWriteState),

        pub fn init(handler: *H, host: []const u8, port: u16, allocator: std.mem.Allocator) !Server(H) {
            var sigmask = posix.empty_sigset;
            linux.sigaddset(&sigmask, posix.SIG.INT);
            linux.sigaddset(&sigmask, posix.SIG.TERM);
            posix.sigprocmask(posix.SIG.BLOCK, &sigmask, null);

            const quit_fd = try posix.signalfd(-1, &sigmask, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK);
            errdefer posix.close(quit_fd);

            const address = try net.Address.resolveIp(host, port);

            const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
            const protocol = posix.IPPROTO.TCP;
            const listener_fd = try posix.socket(address.any.family, socket_type, protocol);

            try posix.setsockopt(listener_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
            try posix.bind(listener_fd, &address.any, address.getOsSockLen());
            try posix.listen(listener_fd, TCP_BACKLOG);

            const epoll_fd = try posix.epoll_create1(0);
            errdefer posix.close(epoll_fd);

            try addFdToEpoll(epoll_fd, listener_fd, EPOLL.IN);
            try addFdToEpoll(epoll_fd, quit_fd, EPOLL.IN);
            log.info("Server listening on {s}:{d}", .{ host, port });

            return .{
                .handler = handler,
                .allocator = allocator,
                .epoll_fd = epoll_fd,
                .listener_fd = listener_fd,
                .quit_fd = quit_fd,
                .pending_writes = std.AutoHashMap(posix.fd_t, PendingWriteState).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.pending_writes.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit();
            }
            self.pending_writes.deinit();
            posix.close(self.listener_fd);
            posix.close(self.quit_fd);
            posix.close(self.epoll_fd);
            log.info("Server shut down gracefully.", .{});
        }

        pub fn run(self: *Self) !void {
            var ready_list: [MAX_EPOLL_EVENTS]linux.epoll_event = undefined;

            main_loop: while (true) {
                var arena = std.heap.ArenaAllocator.init(self.allocator);
                defer arena.deinit();

                const now_us: i64 = std.time.microTimestamp();
                const timeout_ms: c_int = self.handler.getNextTimeoutMs(now_us);

                const events_ready = posix.epoll_wait(self.epoll_fd, &ready_list, timeout_ms);

                for (ready_list[0..events_ready]) |event| {
                    const fd = event.data.fd;

                    if (fd == self.quit_fd) {
                        break :main_loop;
                    } else if (fd == self.listener_fd) {
                        try self.acceptConnection();
                    } else {
                        try self.handleClient(fd, event.events, arena.allocator());
                    }
                }

                const ops = try self.handler.expireDueWaiters(std.time.microTimestamp(), arena.allocator());
                self.writeOps(ops);
            }
        }

        fn handleClient(self: *Self, client_fd: posix.fd_t, events: u32, request_allocator: std.mem.Allocator) !void {
            if ((events & (EPOLL.HUP | EPOLL.ERR)) != 0) {
                return self.closeConnection(client_fd);
            }

            if ((events & EPOLL.OUT) != 0) {
                const done = self.flushPendingWritesForEvent(client_fd);
                if (done) return;
            }

            if ((events & EPOLL.IN) != 0) {
                try self.handleInput(client_fd, request_allocator);
            }
        }

        fn handleInput(self: *Self, client_fd: posix.fd_t, request_allocator: std.mem.Allocator) !void {
            var client_file = std.fs.File{ .handle = client_fd };
            var buffer: [READ_BUFFER_SIZE]u8 = undefined;

            while (true) {
                const bytes_read = client_file.read(&buffer) catch |e| switch (e) {
                    error.WouldBlock => break,
                    else => return self.closeConnection(client_fd),
                };

                const peer_closed = bytes_read == 0;
                if (peer_closed) return self.closeConnection(client_fd);

                const result = try self.handler.ingest(client_fd, buffer[0..bytes_read], request_allocator);
                switch (result) {
                    .Writes => |writes| self.writeOps(writes),
                    .Blocked => {},
                    .Close => return self.closeConnection(client_fd),
                }
            }
        }

        fn writeOps(self: *Self, ops: []WriteOp) void {
            for (ops) |w| {
                self.enqueueWrite(w);
            }
        }

        fn acceptConnection(self: *Self) !void {
            const client_fd = try posix.accept(self.listener_fd, null, null, posix.SOCK.NONBLOCK);
            errdefer posix.close(client_fd);

            try addFdToEpoll(self.epoll_fd, client_fd, linux.EPOLL.IN);
            try self.handler.addConnection(client_fd);
            errdefer self.handler.removeConnection(client_fd);

            var write_state = PendingWriteState.init(self.allocator);
            errdefer write_state.deinit();
            try self.pending_writes.put(client_fd, write_state);

            log.info("Accepted connection: fd={d}", .{client_fd});
        }

        fn closeConnection(self: *Self, client_fd: posix.fd_t) void {
            if (self.pending_writes.fetchRemove(client_fd)) |entry| {
                var removed_state = entry.value;
                removed_state.deinit();
            }
            self.handler.removeConnection(client_fd);
            posix.close(client_fd);
            log.info("Closed connection: fd={d}", .{client_fd});
        }

        fn enqueueWrite(self: *Self, op: WriteOp) void {
            const state_ptr = self.pending_writes.getPtr(op.fd) orelse return;

            state_ptr.buffer.appendSlice(op.bytes) catch {
                self.closeConnection(op.fd);
                return;
            };

            switch (self.flushPendingWrites(op.fd, state_ptr)) {
                .Idle => {},
                .Pending => {},
                .Closed => self.closeConnection(op.fd),
            }
        }

        fn flushPendingWritesForEvent(self: *Self, fd: posix.fd_t) bool {
            const state_ptr = self.pending_writes.getPtr(fd) orelse return false;

            return switch (self.flushPendingWrites(fd, state_ptr)) {
                .Closed => blk: {
                    self.closeConnection(fd);
                    break :blk true;
                },
                else => false,
            };
        }

        fn flushPendingWrites(self: *Self, fd: posix.fd_t, state: *PendingWriteState) FlushResult {
            while (state.hasPendingData()) {
                const slice = state.buffer.items[state.written..];
                const bytes_written = posix.write(fd, slice) catch |err| switch (err) {
                    error.WouldBlock => {
                        self.setWriteInterest(fd, state, true) catch return .Closed;
                        return .Pending;
                    },
                    else => return .Closed,
                };

                if (bytes_written == 0) {
                    return .Closed;
                }

                state.written += bytes_written;
            }

            state.buffer.clearRetainingCapacity();
            state.written = 0;
            self.setWriteInterest(fd, state, false) catch return .Closed;
            return .Idle;
        }

        fn setWriteInterest(self: *Self, fd: posix.fd_t, state: *PendingWriteState, enabled: bool) !void {
            if (state.write_interest == enabled) return;

            var events: u32 = EPOLL.IN;
            if (enabled) events |= EPOLL.OUT;
            var event = linux.epoll_event{
                .events = events,
                .data = .{ .fd = fd },
            };
            try posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_MOD, fd, &event);
            state.write_interest = enabled;
        }
    };
}

fn addFdToEpoll(epoll_fd: posix.fd_t, fd: posix.fd_t, events: u32) !void {
    var event = linux.epoll_event{
        .events = events,
        .data = .{ .fd = fd },
    };
    try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, fd, &event);
}
