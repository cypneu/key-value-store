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

        fd_to_id: std.AutoHashMap(posix.fd_t, u64),
        id_to_fd: std.AutoHashMap(u64, posix.fd_t),

        connecting_master_id: ?u64 = null,
        master_listening_port: ?u16 = null,

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
                .fd_to_id = std.AutoHashMap(posix.fd_t, u64).init(allocator),
                .id_to_fd = std.AutoHashMap(u64, posix.fd_t).init(allocator),
                .connecting_master_id = null,
                .master_listening_port = null,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.pending_writes.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit();
            }
            self.pending_writes.deinit();
            self.fd_to_id.deinit();
            self.id_to_fd.deinit();
            posix.close(self.listener_fd);
            posix.close(self.quit_fd);
            posix.close(self.epoll_fd);
            log.info("Server shut down gracefully.", .{});
        }

        pub fn connectToMaster(self: *Self, host: []const u8, port: u16, listening_port: u16) !void {
            const addrs = try net.getAddressList(self.allocator, host, port);
            defer addrs.deinit();

            var connect_err: anyerror = error.ConnectionRefused;

            for (addrs.addrs) |address| {
                self.attemptMasterConnection(address, listening_port) catch |err| {
                    connect_err = err;
                    continue;
                };
                return;
            }

            return connect_err;
        }

        fn attemptMasterConnection(self: *Self, address: net.Address, listening_port: u16) !void {
            const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
            const protocol = posix.IPPROTO.TCP;
            const fd = try posix.socket(address.any.family, socket_type, protocol);
            errdefer posix.close(fd);

            _ = posix.connect(fd, &address.any, address.getOsSockLen()) catch |err| {
                if (err != error.WouldBlock) return err;
            };

            const conn_id = try self.handler.createConnection();
            errdefer self.handler.removeConnection(conn_id);

            try self.registerConnection(fd, conn_id);

            self.connecting_master_id = conn_id;
            self.master_listening_port = listening_port;

            if (self.pending_writes.getPtr(fd)) |state| {
                try self.setWriteInterest(fd, state, true);
            }
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
                    const action = self.processEvent(event.data.fd);
                    switch (action) {
                        .stop_server => break :main_loop,
                        .client_connected => try self.acceptConnection(),
                        .master_connected => try self.handleMasterConnect(event.data.fd, arena.allocator()),
                        .handle_client => try self.handleClient(event.data.fd, event.events, arena.allocator()),
                    }
                }

                const ops = try self.handler.expireDueWaiters(std.time.microTimestamp(), arena.allocator());
                self.writeOps(ops);
            }
        }

        const EventAction = enum { master_connected, client_connected, stop_server, handle_client };

        fn processEvent(self: *Self, fd: i32) EventAction {
            if (fd == self.quit_fd) {
                return .stop_server;
            } else if (fd == self.listener_fd) {
                return .client_connected;
            } else if (self.isConnectingMaster(fd)) {
                return .master_connected;
            } else {
                return .handle_client;
            }
        }

        fn isConnectingMaster(self: *Self, fd: i32) bool {
            const mid = self.connecting_master_id orelse return false;
            const mfd = self.id_to_fd.get(mid) orelse return false;
            return fd == mfd;
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

        fn handleMasterConnect(self: *Self, fd: posix.fd_t, allocator: std.mem.Allocator) !void {
            posix.getsockoptError(fd) catch |err| {
                log.err("Failed to connect to master: {}", .{err});
                return self.closeConnection(fd);
            };

            log.info("Successfully connected to master", .{});

            const conn_id = self.connecting_master_id.?;
            self.connecting_master_id = null;
            const port = self.master_listening_port.?;

            const writes = try self.handler.registerMasterConnection(allocator, conn_id, port);
            self.writeOps(writes);
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

                const conn_id = self.fd_to_id.get(client_fd) orelse {
                    return self.closeConnection(client_fd);
                };

                const result = try self.handler.ingest(conn_id, buffer[0..bytes_read], request_allocator);
                switch (result) {
                    .Writes => |writes| self.writeOps(writes),
                    .Blocked => {},
                    .Close => return self.closeConnection(client_fd),
                }
            }
        }

        fn writeOps(self: *Self, ops: []WriteOp) void {
            for (ops) |w| {
                if (self.id_to_fd.get(w.connection_id)) |fd| {
                    self.enqueueWrite(fd, w.bytes);
                }
            }
        }

        fn acceptConnection(self: *Self) !void {
            const client_fd = try posix.accept(self.listener_fd, null, null, posix.SOCK.NONBLOCK);
            errdefer posix.close(client_fd);

            const conn_id = try self.handler.createConnection();
            try self.registerConnection(client_fd, conn_id);

            log.info("Accepted connection: fd={d} id={d}", .{ client_fd, conn_id });
        }

        fn registerConnection(self: *Self, fd: posix.fd_t, id: u64) !void {
            try addFdToEpoll(self.epoll_fd, fd, linux.EPOLL.IN);

            try self.fd_to_id.put(fd, id);
            try self.id_to_fd.put(id, fd);

            var write_state = PendingWriteState.init(self.allocator);
            errdefer write_state.deinit();
            try self.pending_writes.put(fd, write_state);
        }

        fn closeConnection(self: *Self, client_fd: posix.fd_t) void {
            if (self.pending_writes.fetchRemove(client_fd)) |entry| {
                var removed_state = entry.value;
                removed_state.deinit();
            }

            if (self.fd_to_id.fetchRemove(client_fd)) |entry| {
                const conn_id = entry.value;
                _ = self.id_to_fd.remove(conn_id);
                self.handler.removeConnection(conn_id);
            }

            posix.close(client_fd);
            log.info("Closed connection: fd={d}", .{client_fd});
        }

        fn enqueueWrite(self: *Self, fd: posix.fd_t, bytes: []const u8) void {
            const state_ptr = self.pending_writes.getPtr(fd) orelse return;

            state_ptr.buffer.appendSlice(bytes) catch {
                self.closeConnection(fd);
                return;
            };

            switch (self.flushPendingWrites(fd, state_ptr)) {
                .Idle => {},
                .Pending => {},
                .Closed => self.closeConnection(fd),
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
