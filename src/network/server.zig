const std = @import("std");
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;

const event_loop = @import("event_loop.zig");
const reply_writer = @import("resp/writer.zig");

const Services = @import("../services.zig").Services;
const Config = @import("../config.zig").Config;
const WriteOp = @import("../services.zig").WriteOp;
const SnapshotRegistration = @import("../features/replication.zig").SnapshotRegistration;
const EventLoop = event_loop.EventLoop;

const log = std.log.scoped(.server);

const READ_BUFFER_SIZE = 16 * 1024;

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

const SnapshotState = struct {
    pipe_fd: posix.fd_t,
    connection_id: u64,
    child_pid: posix.pid_t,
    buffer: std.ArrayList(u8),

    fn init(allocator: std.mem.Allocator, pipe_fd: posix.fd_t, connection_id: u64, child_pid: std.posix.pid_t) SnapshotState {
        return .{
            .pipe_fd = pipe_fd,
            .connection_id = connection_id,
            .child_pid = child_pid,
            .buffer = std.ArrayList(u8).init(allocator),
        };
    }

    fn deinit(self: *SnapshotState) void {
        self.buffer.deinit();
    }
};

const FlushResult = enum { Idle, Pending, Closed };

pub const Server = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: Config,

    event_loop: EventLoop,

    fd_to_id: std.AutoHashMap(posix.fd_t, u64),
    id_to_fd: std.AutoHashMap(u64, posix.fd_t),

    pending_writes: std.AutoHashMap(posix.fd_t, PendingWriteState),

    active_snapshot_pipe: ?SnapshotState,

    services: Services,

    pub fn init(config: Config) !Server {
        var el = try EventLoop.init(config.allocator, config.host, config.port);
        errdefer el.deinit();

        return .{
            .allocator = config.allocator,
            .config = config,
            .event_loop = el,
            .fd_to_id = std.AutoHashMap(posix.fd_t, u64).init(config.allocator),
            .id_to_fd = std.AutoHashMap(u64, posix.fd_t).init(config.allocator),
            .pending_writes = std.AutoHashMap(posix.fd_t, PendingWriteState).init(config.allocator),
            .active_snapshot_pipe = null,
            .services = Services.init(config),
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

        if (self.active_snapshot_pipe) |*snap| {
            snap.deinit();
            posix.close(snap.pipe_fd);
        }

        self.event_loop.deinit();
        self.services.deinit();
        log.info("Server shut down gracefully.", .{});
    }

    pub fn run(self: *Self) !void {
        main_loop: while (true) {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();

            const now_us: i64 = std.time.microTimestamp();
            const timeout_ms = self.services.blocking.getNextTimeoutMs(now_us);

            var events = self.event_loop.wait(timeout_ms);

            while (events.next()) |event| {
                switch (event) {
                    .shutdown => break :main_loop,
                    .accept => try self.acceptConnection(),
                    .pipe_readable => |fd| try self.handleSnapshotPipe(fd, arena.allocator()),
                    .master_connected => |fd| try self.handleMasterConnect(fd, arena.allocator()),
                    .client_io => |io_event| try self.handleClientIo(io_event, arena.allocator()),
                }
            }

            const ops = try self.services.tick(std.time.microTimestamp(), arena.allocator());
            self.writeOps(ops);
        }
    }

    pub fn connectToMaster(self: *Self, host: []const u8, port: u16) !void {
        const addrs = try net.getAddressList(self.allocator, host, port);
        defer addrs.deinit();

        var connect_err: anyerror = error.ConnectionRefused;

        for (addrs.addrs) |address| {
            self.attemptMasterConnection(address) catch |err| {
                connect_err = err;
                continue;
            };
            return;
        }

        return connect_err;
    }

    fn attemptMasterConnection(self: *Self, address: net.Address) !void {
        const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
        const protocol = posix.IPPROTO.TCP;
        const fd = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(fd);

        _ = posix.connect(fd, &address.any, address.getOsSockLen()) catch |err| {
            if (err != error.WouldBlock) return err;
        };

        const conn_id = try self.services.connections.create();
        errdefer _ = self.services.removeConnection(conn_id);

        try self.event_loop.registerFd(fd, .master_connecting, .{ .readable = true, .writable = true });
        try self.registerFd(fd, conn_id);
    }

    fn acceptConnection(self: *Self) !void {
        const client_fd = try self.event_loop.acceptClient();
        errdefer posix.close(client_fd);

        const conn_id = try self.services.connections.create();
        errdefer _ = self.services.removeConnection(conn_id);

        try self.event_loop.registerFd(client_fd, .client, .{ .readable = true, .writable = false });
        try self.registerFd(client_fd, conn_id);
        log.info("Accepted connection: fd={d} id={d}", .{ client_fd, conn_id });
    }

    fn registerFd(self: *Self, fd: posix.fd_t, conn_id: u64) !void {
        try self.fd_to_id.put(fd, conn_id);
        try self.id_to_fd.put(conn_id, fd);

        var write_state = PendingWriteState.init(self.allocator);
        errdefer write_state.deinit();
        try self.pending_writes.put(fd, write_state);
    }

    fn handleClientIo(self: *Self, io_event: event_loop.ClientIoEvent, request_allocator: std.mem.Allocator) !void {
        const client_fd = io_event.fd;

        if (io_event.hup or io_event.err) {
            self.closeConnection(client_fd);
            return;
        }

        if (io_event.writable) {
            if (self.flushPendingWritesForFd(client_fd)) {
                self.closeConnection(client_fd);
                return;
            }
        }

        if (io_event.readable) {
            try self.handleInput(client_fd, request_allocator);
        }
    }

    fn handleMasterConnect(self: *Self, fd: posix.fd_t, allocator: std.mem.Allocator) !void {
        posix.getsockoptError(fd) catch |err| {
            log.err("Failed to connect to master: {}", .{err});
            self.closeConnection(fd);
            return;
        };

        log.info("Successfully connected to master", .{});

        const conn_id = self.fd_to_id.get(fd).?;
        self.event_loop.promoteMasterToClient(fd);

        const writes = try self.services.replication.registerMasterConnection(allocator, conn_id, self.config.port);
        self.writeOps(writes);
    }

    fn handleInput(self: *Self, client_fd: posix.fd_t, request_allocator: std.mem.Allocator) !void {
        var client_file = std.fs.File{ .handle = client_fd };
        var buffer: [READ_BUFFER_SIZE]u8 = undefined;

        while (true) {
            const bytes_read = client_file.read(&buffer) catch |e| switch (e) {
                error.WouldBlock => break,
                else => {
                    self.closeConnection(client_fd);
                    return;
                },
            };

            const peer_closed = bytes_read == 0;
            if (peer_closed) {
                self.closeConnection(client_fd);
                return;
            }

            const conn_id = self.fd_to_id.get(client_fd) orelse {
                self.closeConnection(client_fd);
                return;
            };

            const result = self.services.processBytes(conn_id, buffer[0..bytes_read], request_allocator) catch |err| {
                if (err == error.ProtocolError) {
                    self.closeConnection(client_fd);
                    return;
                }
                return err;
            };

            self.writeOps(result.writes);
            if (result.snapshot) |snap| try self.registerSnapshot(snap);
        }
    }

    fn registerSnapshot(self: *Self, snap: SnapshotRegistration) !void {
        self.services.replication.setLoadingSnapshot(snap.connection_id, true);

        self.active_snapshot_pipe = SnapshotState.init(
            self.allocator,
            snap.read_fd,
            snap.connection_id,
            snap.child_pid,
        );
        try self.event_loop.registerFd(snap.read_fd, .pipe, .{ .readable = true, .writable = false });
    }

    fn writeOps(self: *Self, ops: []const WriteOp) void {
        var current_fd: ?posix.fd_t = null;

        for (ops) |w| {
            if (self.id_to_fd.get(w.connection_id)) |fd| {
                if (current_fd) |curr| {
                    if (curr != fd) {
                        _ = self.flushPendingWritesForFd(curr);
                        current_fd = fd;
                    }
                } else {
                    current_fd = fd;
                }

                const bytes = reply_writer.renderReply(self.services.allocator, w.reply) catch {
                    log.err("Failed to render reply", .{});
                    continue;
                };
                defer self.services.allocator.free(bytes);
                self.appendWriteBuffer(fd, bytes);
            }
        }

        if (current_fd) |fd| {
            _ = self.flushPendingWritesForFd(fd);
        }
    }

    fn appendWriteBuffer(self: *Self, fd: posix.fd_t, bytes: []const u8) void {
        const state_ptr = self.pending_writes.getPtr(fd) orelse return;

        state_ptr.buffer.appendSlice(bytes) catch {
            self.closeConnection(fd);
            return;
        };
    }

    fn flushPendingWritesForFd(self: *Self, fd: posix.fd_t) bool {
        const state_ptr = self.pending_writes.getPtr(fd) orelse return false;

        return switch (self.flushPendingWrites(fd, state_ptr)) {
            .Closed => true,
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

        try self.event_loop.modifyInterest(fd, .{ .readable = true, .writable = enabled });
        state.write_interest = enabled;
    }

    fn closeConnection(self: *Self, client_fd: posix.fd_t) void {
        if (self.pending_writes.fetchRemove(client_fd)) |entry| {
            var removed_state = entry.value;
            removed_state.deinit();
        }

        if (self.fd_to_id.fetchRemove(client_fd)) |entry| {
            const conn_id = entry.value;
            _ = self.id_to_fd.remove(conn_id);

            const cleanup = self.services.removeConnection(conn_id);
            if (cleanup.snapshot_orphaned) {
                self.cleanupSnapshotPipe();
            }
        }

        self.event_loop.unregisterFd(client_fd);
        posix.close(client_fd);
        log.info("Closed connection: fd={d}", .{client_fd});
    }

    fn cleanupSnapshotPipe(self: *Self) void {
        if (self.active_snapshot_pipe) |*snap| {
            _ = posix.waitpid(snap.child_pid, 0);
            self.event_loop.unregisterFd(snap.pipe_fd);
            posix.close(snap.pipe_fd);
            snap.deinit();
            self.active_snapshot_pipe = null;
        }
    }

    fn handleSnapshotPipe(self: *Self, pipe_fd: posix.fd_t, alloc: std.mem.Allocator) !void {
        const snap_ptr = &(self.active_snapshot_pipe orelse {
            posix.close(pipe_fd);
            return;
        });

        const conn_fd = self.id_to_fd.get(snap_ptr.connection_id) orelse {
            self.cleanupSnapshotPipe();
            return;
        };

        var buf: [4096]u8 = undefined;
        const nread = posix.read(pipe_fd, &buf) catch |err| switch (err) {
            error.WouldBlock => return,
            else => {
                self.cleanupSnapshotPipe();
                self.closeConnection(conn_fd);
                return;
            },
        };

        if (nread > 0) {
            snap_ptr.buffer.appendSlice(buf[0..nread]) catch {
                self.cleanupSnapshotPipe();
                self.closeConnection(conn_fd);
                return;
            };
            return;
        }

        self.sendSnapshotToReplicas(snap_ptr.buffer.items, alloc);
        self.cleanupSnapshotPipe();
    }

    fn sendSnapshotToReplicas(self: *Self, snapshot_data: []const u8, alloc: std.mem.Allocator) void {
        var header_buf: [32]u8 = undefined;
        const header = std.fmt.bufPrint(&header_buf, "${d}\r\n", .{snapshot_data.len}) catch unreachable;

        const transfer = self.services.replication.completeSnapshotTransfer(alloc) catch return;
        defer alloc.free(transfer.replicas);

        for (transfer.replicas) |replica| {
            const replica_fd = self.id_to_fd.get(replica.connection_id) orelse continue;

            self.appendWriteBuffer(replica_fd, header);
            self.appendWriteBuffer(replica_fd, snapshot_data);
            self.appendWriteBuffer(replica_fd, "\r\n");

            defer {
                for (replica.buffered_commands) |b| self.allocator.free(b);
                alloc.free(replica.buffered_commands);
            }

            for (replica.buffered_commands) |bytes| {
                self.appendWriteBuffer(replica_fd, bytes);
            }

            _ = self.flushPendingWritesForFd(replica_fd);
        }
    }
};
