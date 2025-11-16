const std = @import("std");
const db = @import("data_structures/mod.zig");
const dispatcher = @import("dispatcher.zig");
const posix = std.posix;
const format = @import("resp/format.zig");
const Reply = @import("reply.zig").Reply;
const Command = @import("command.zig").Command;
const Order = std.math.Order;
const StreamParser = @import("resp/stream_parser.zig").StreamParser;
const StreamReadRequest = @import("handlers.zig").StreamReadRequest;

pub const WriteOp = struct {
    fd: std.posix.fd_t,
    bytes: []const u8,
};

pub const IngestResult = union(enum) {
    Writes: []WriteOp,
    Blocked,
    Close,
};

pub const ClientConnection = struct {
    fd: posix.fd_t,
    blocking_key: ?[]const u8,
    blocking_node: ?*std.DoublyLinkedList(posix.fd_t).Node,
    deadline_us: ?i64,
    blocking_stream_state: ?StreamBlockingState,
    in_transaction: bool,
    transaction_queue: std.ArrayList(TransactionCommand),

    read_buffer: std.ArrayList(u8),
    read_start: usize,
    parser: StreamParser,
};

const StreamWaitList = std.DoublyLinkedList(posix.fd_t);

const StreamRegistration = struct {
    key: []const u8,
    node: *StreamWaitList.Node,
};

const StreamBlockingState = struct {
    requests: []StreamReadRequest,
    registrations: []StreamRegistration,
};

pub const TransactionCommand = struct {
    command: Command,
    parts: [][]const u8,
};

pub const AppHandler = struct {
    app_allocator: std.mem.Allocator,
    string_store: *db.StringStore,
    list_store: *db.ListStore,
    stream_store: *db.StreamStore,

    connection_by_fd: std.hash_map.AutoHashMap(posix.fd_t, ClientConnection),
    blocked_clients_by_key: std.hash_map.StringHashMap(std.DoublyLinkedList(posix.fd_t)),
    blocked_stream_clients_by_key: std.hash_map.StringHashMap(StreamWaitList),

    timeouts: TimeoutQueue,

    const TimeoutEntry = struct {
        deadline_us: i64,
        fd: posix.fd_t,
    };

    fn timeoutCompare(_: void, a: TimeoutEntry, b: TimeoutEntry) Order {
        return std.math.order(a.deadline_us, b.deadline_us);
    }

    const TimeoutQueue = std.PriorityQueue(TimeoutEntry, void, timeoutCompare);

    pub fn init(
        allocator: std.mem.Allocator,
        string_store: *db.StringStore,
        list_store: *db.ListStore,
        stream_store: *db.StreamStore,
    ) AppHandler {
        const connection_by_fd = std.hash_map.AutoHashMap(posix.fd_t, ClientConnection).init(allocator);
        const blocked_clients_by_key = std.hash_map.StringHashMap(std.DoublyLinkedList(posix.fd_t)).init(allocator);
        const blocked_stream_clients_by_key = std.hash_map.StringHashMap(StreamWaitList).init(allocator);
        const timeouts = TimeoutQueue.init(allocator, {});

        return .{
            .app_allocator = allocator,
            .string_store = string_store,
            .list_store = list_store,
            .stream_store = stream_store,
            .connection_by_fd = connection_by_fd,
            .blocked_clients_by_key = blocked_clients_by_key,
            .blocked_stream_clients_by_key = blocked_stream_clients_by_key,
            .timeouts = timeouts,
        };
    }

    pub fn deinit(self: *AppHandler) void {
        var it_conn = self.connection_by_fd.iterator();
        while (it_conn.next()) |entry| {
            entry.value_ptr.read_buffer.deinit();
            self.clearTransactionQueue(entry.value_ptr);
            entry.value_ptr.transaction_queue.deinit();
        }
        var it = self.blocked_clients_by_key.iterator();
        while (it.next()) |entry| {
            self.app_allocator.free(entry.key_ptr.*);
        }
        self.blocked_clients_by_key.deinit();
        var stream_it = self.blocked_stream_clients_by_key.iterator();
        while (stream_it.next()) |entry| {
            self.app_allocator.free(entry.key_ptr.*);
        }
        self.blocked_stream_clients_by_key.deinit();

        self.connection_by_fd.deinit();
        self.timeouts.deinit();
    }

    pub fn ingest(self: *AppHandler, client_fd: posix.fd_t, incoming: []const u8, request_allocator: std.mem.Allocator) !IngestResult {
        const conn = self.connection_by_fd.getPtr(client_fd) orelse return .Close;

        try conn.read_buffer.appendSlice(incoming);

        var notifications = std.ArrayList(WriteOp).init(request_allocator);

        while (true) {
            const available = conn.read_buffer.items[conn.read_start..];
            const outcome = conn.parser.parse(available);
            switch (outcome) {
                .NeedMore => break,
                .Error => return .Close,
                .Done => |done| {
                    const single = try dispatcher.dispatchCommand(self, request_allocator, done.parts, conn);
                    switch (single) {
                        .Immediate => |value| {
                            try notifications.append(.{ .fd = client_fd, .bytes = value.bytes });
                            if (value.notify) |ns| try notifications.appendSlice(ns);
                        },
                        .Blocked => {},
                    }

                    conn.read_start += done.consumed;
                    conn.parser.reset();

                    if (conn.read_start == conn.read_buffer.items.len) {
                        conn.read_buffer.clearRetainingCapacity();
                        conn.read_start = 0;
                        break;
                    }
                },
            }
        }

        if (notifications.items.len != 0) {
            return .{ .Writes = try notifications.toOwnedSlice() };
        }
        return .Blocked;
    }

    pub fn addConnection(self: *AppHandler, client_fd: posix.fd_t) !void {
        try self.connection_by_fd.put(client_fd, ClientConnection{
            .fd = client_fd,
            .blocking_key = null,
            .blocking_node = null,
            .deadline_us = null,
            .blocking_stream_state = null,
            .in_transaction = false,
            .transaction_queue = std.ArrayList(TransactionCommand).init(self.app_allocator),
            .read_buffer = std.ArrayList(u8).init(self.app_allocator),
            .read_start = 0,
            .parser = StreamParser.init(),
        });
    }

    pub fn removeConnection(self: *AppHandler, client_fd: posix.fd_t) void {
        if (self.connection_by_fd.fetchRemove(client_fd)) |removed_connection| {
            var conn = removed_connection.value;

            conn.read_buffer.deinit();
            self.clearTransactionQueue(&conn);
            conn.transaction_queue.deinit();

            if (conn.blocking_key) |key| {
                const wait_list = self.blocked_clients_by_key.getPtr(key).?;
                const node_ptr = conn.blocking_node.?;
                wait_list.remove(node_ptr);
                self.app_allocator.destroy(node_ptr);
            }

            self.cleanupStreamBlockingState(conn.blocking_stream_state);
        }
    }

    fn cleanupStreamBlockingState(self: *AppHandler, maybe_state: ?StreamBlockingState) void {
        if (maybe_state) |state| {
            for (state.registrations) |registration| {
                if (self.blocked_stream_clients_by_key.getPtr(registration.key)) |wait_list| {
                    wait_list.remove(registration.node);
                }
                self.app_allocator.destroy(registration.node);
            }

            for (state.requests) |request| {
                self.app_allocator.free(request.id);
            }

            self.app_allocator.free(state.registrations);
            self.app_allocator.free(state.requests);
        }
    }

    pub fn clearStreamBlockingState(self: *AppHandler, conn: *ClientConnection) void {
        self.cleanupStreamBlockingState(conn.blocking_stream_state);
        conn.blocking_stream_state = null;
        conn.deadline_us = null;
    }

    pub fn clearTransactionQueue(self: *AppHandler, conn: *ClientConnection) void {
        for (conn.transaction_queue.items) |queued| {
            for (queued.parts) |part| {
                self.app_allocator.free(part);
            }
            self.app_allocator.free(queued.parts);
        }
        conn.transaction_queue.clearRetainingCapacity();
    }

    fn ensureStreamWaitList(self: *AppHandler, key: []const u8) !struct {
        key: []const u8,
        wait_list: *StreamWaitList,
    } {
        const gop = try self.blocked_stream_clients_by_key.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.app_allocator.dupe(u8, key);
            gop.value_ptr.* = .{};
        }
        return .{ .key = gop.key_ptr.*, .wait_list = gop.value_ptr };
    }

    fn cleanupStreamSlices(self: *AppHandler, requests: []StreamReadRequest, registrations: []StreamRegistration) void {
        for (registrations) |registration| {
            if (self.blocked_stream_clients_by_key.getPtr(registration.key)) |wait_list| {
                wait_list.remove(registration.node);
            }
            self.app_allocator.destroy(registration.node);
        }

        for (requests) |request| {
            self.app_allocator.free(request.id);
        }
    }

    fn createStreamBlockingState(self: *AppHandler, connection: *ClientConnection, requests: []const StreamReadRequest) !StreamBlockingState {
        const count = requests.len;
        const stored_requests = try self.app_allocator.alloc(StreamReadRequest, count);
        errdefer self.app_allocator.free(stored_requests);

        const registrations = try self.app_allocator.alloc(StreamRegistration, count);
        errdefer self.app_allocator.free(registrations);

        var created: usize = 0;
        errdefer self.cleanupStreamSlices(stored_requests[0..created], registrations[0..created]);

        for (requests, 0..) |request, idx| {
            const wait = try self.ensureStreamWaitList(request.key);
            const node_ptr = try self.app_allocator.create(StreamWaitList.Node);
            node_ptr.* = .{ .data = connection.fd };
            wait.wait_list.append(node_ptr);

            stored_requests[idx] = .{
                .key = wait.key,
                .id = try self.app_allocator.dupe(u8, request.id),
            };
            registrations[idx] = .{ .key = wait.key, .node = node_ptr };
            created = idx + 1;
        }

        return .{ .requests = stored_requests, .registrations = registrations };
    }

    fn setStreamBlockDeadline(self: *AppHandler, connection: *ClientConnection, block_ms: ?u64) !void {
        if (block_ms) |ms| {
            if (ms == 0) {
                connection.deadline_us = null;
                return;
            }
            const now_us: i64 = std.time.microTimestamp();
            const delta_us: i64 = @intCast(ms * @as(u64, std.time.us_per_ms));
            const deadline_us = now_us + delta_us;
            connection.deadline_us = deadline_us;
            try self.timeouts.add(.{ .deadline_us = deadline_us, .fd = connection.fd });
        } else {
            connection.deadline_us = null;
        }
    }

    pub fn registerStreamBlockingClient(
        self: *AppHandler,
        connection: *ClientConnection,
        requests: []const StreamReadRequest,
        block_ms: ?u64,
    ) !void {
        const state = try self.createStreamBlockingState(connection, requests);
        errdefer self.cleanupStreamBlockingState(state);

        connection.blocking_stream_state = state;
        connection.blocking_key = null;
        connection.blocking_node = null;
        try self.setStreamBlockDeadline(connection, block_ms);
    }

    pub fn drainWaitersForKey(self: *AppHandler, request_allocator: std.mem.Allocator, key: []const u8, pushed: usize) ![]WriteOp {
        const wait_list_ptr = self.blocked_clients_by_key.getPtr(key) orelse return &.{};

        var notifications = std.ArrayList(WriteOp).init(request_allocator);
        errdefer notifications.deinit();

        for (0..pushed) |_| {
            const node_ptr = wait_list_ptr.popFirst() orelse break;

            const popped_values = try self.list_store.lpop(key, 1, request_allocator);
            if (popped_values.len == 0) {
                wait_list_ptr.prepend(node_ptr);
                break;
            }

            const fd = node_ptr.data;
            defer self.app_allocator.destroy(node_ptr);

            if (self.connection_by_fd.getPtr(fd)) |conn| {
                conn.blocking_key = null;
                conn.blocking_node = null;
                conn.deadline_us = null;
            }

            var buf = std.ArrayList(u8).init(request_allocator);
            var replies: [2]Reply = .{
                .{ .BulkString = key },
                .{ .BulkString = popped_values[0] },
            };

            try format.writeReply(buf.writer(), Reply{ .Array = &replies });
            const bytes = try buf.toOwnedSlice();

            try notifications.append(.{ .fd = fd, .bytes = bytes });
        }

        return try notifications.toOwnedSlice();
    }

    pub fn expireDueWaiters(self: *AppHandler, now_us: i64, request_allocator: std.mem.Allocator) ![]WriteOp {
        var notifications = std.ArrayList(WriteOp).init(request_allocator);

        while (self.timeouts.peek()) |t| {
            if (t.deadline_us > now_us) break;

            const entry = self.timeouts.remove();
            const fd = entry.fd;

            if (self.connection_by_fd.getPtr(fd)) |conn| {
                const maybe_key = conn.blocking_key;
                const maybe_node = conn.blocking_node;
                if (maybe_key) |key| {
                    if (maybe_node) |node_ptr| {
                        if (self.blocked_clients_by_key.getPtr(key)) |wait_list| {
                            wait_list.remove(node_ptr);
                        }
                        self.app_allocator.destroy(node_ptr);
                    }

                    conn.blocking_key = null;
                    conn.blocking_node = null;
                    conn.deadline_us = null;

                    var buf = std.ArrayList(u8).init(request_allocator);
                    try format.writeReply(buf.writer(), Reply{ .Array = null });
                    const bytes = try buf.toOwnedSlice();
                    try notifications.append(.{ .fd = fd, .bytes = bytes });
                } else if (conn.blocking_stream_state != null) {
                    var buf = std.ArrayList(u8).init(request_allocator);
                    try format.writeReply(buf.writer(), Reply{ .Array = null });
                    const bytes = try buf.toOwnedSlice();
                    try notifications.append(.{ .fd = fd, .bytes = bytes });
                    self.clearStreamBlockingState(conn);
                }
            }
        }

        return try notifications.toOwnedSlice();
    }

    pub fn getNextTimeoutMs(self: *AppHandler, now_us: i64) c_int {
        if (self.timeouts.peek()) |t| {
            if (t.deadline_us <= now_us) return 0;
            const delta_us: i64 = t.deadline_us - now_us;
            const delta_ms_i64: i64 = @divTrunc(delta_us, @as(i64, @intCast(std.time.us_per_ms)));
            const max_cint: c_int = std.math.maxInt(c_int);
            if (delta_ms_i64 > max_cint) return max_cint;
            if (delta_ms_i64 < 0) return 0;
            return @intCast(delta_ms_i64);
        }
        return -1;
    }
};
