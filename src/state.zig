const std = @import("std");
const db = @import("data_structures/mod.zig");
const dispatcher = @import("dispatcher.zig");
const format = @import("resp/format.zig");
const Reply = @import("reply.zig").Reply;
const Command = @import("command.zig").Command;
const Order = std.math.Order;
const StreamParser = @import("resp/stream_parser.zig").StreamParser;
const StreamReadRequest = @import("handlers.zig").StreamReadRequest;

pub const WriteOp = struct {
    connection_id: u64,
    bytes: []const u8,
};

pub const ServerRole = enum { master, replica };

pub const ReplicaConfig = struct {
    host: []const u8,
    port: u16,
};

pub const IngestResult = union(enum) {
    Writes: []WriteOp,
    Blocked,
    Close,
};

pub const ClientConnection = struct {
    id: u64,
    blocking_key: ?[]const u8,
    blocking_node: ?*std.DoublyLinkedList(u64).Node,
    deadline_us: ?i64,
    blocking_stream_state: ?StreamBlockingState,
    wait_state: ?WaitState,
    in_transaction: bool,
    transaction_queue: std.ArrayList(TransactionCommand),
    is_replica: bool,

    read_buffer: std.ArrayList(u8),
    read_start: usize,
    parser: StreamParser,
};

const StreamWaitList = std.DoublyLinkedList(u64);

const StreamRegistration = struct {
    key: []const u8,
    node: *StreamWaitList.Node,
};

const StreamBlockingState = struct {
    requests: []StreamReadRequest,
    registrations: []StreamRegistration,
};

const WaitState = struct {
    required: u64,
    target_offset: u64,
};

const WaitRegistration = struct {
    connection_id: u64,
    required: u64,
    target_offset: u64,
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
    role: ServerRole,
    dir: []const u8,
    db_filename: []const u8,

    master: ?ReplicaConfig,
    master_connection_id: ?u64,
    replication_offset: u64,

    connection_by_id: std.hash_map.AutoHashMap(u64, ClientConnection),
    blocked_clients_by_key: std.hash_map.StringHashMap(std.DoublyLinkedList(u64)),
    blocked_stream_clients_by_key: std.hash_map.StringHashMap(StreamWaitList),
    replicas: std.ArrayList(u64),
    replica_acks: std.hash_map.AutoHashMap(u64, u64),
    waiters: std.ArrayList(WaitRegistration),

    next_connection_id: u64,

    timeouts: TimeoutQueue,

    const TimeoutEntry = struct {
        deadline_us: i64,
        connection_id: u64,
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
        role: ServerRole,
        master: ?ReplicaConfig,
        dir: []const u8,
        db_filename: []const u8,
    ) AppHandler {
        const connection_by_id = std.hash_map.AutoHashMap(u64, ClientConnection).init(allocator);
        const blocked_clients_by_key = std.hash_map.StringHashMap(std.DoublyLinkedList(u64)).init(allocator);
        const blocked_stream_clients_by_key = std.hash_map.StringHashMap(StreamWaitList).init(allocator);
        const replicas = std.ArrayList(u64).init(allocator);
        const timeouts = TimeoutQueue.init(allocator, {});

        return .{
            .app_allocator = allocator,
            .string_store = string_store,
            .list_store = list_store,
            .stream_store = stream_store,
            .role = role,
            .dir = dir,
            .db_filename = db_filename,
            .master = master,
            .master_connection_id = null,
            .replication_offset = 0,
            .connection_by_id = connection_by_id,
            .blocked_clients_by_key = blocked_clients_by_key,
            .blocked_stream_clients_by_key = blocked_stream_clients_by_key,
            .replicas = replicas,
            .replica_acks = std.hash_map.AutoHashMap(u64, u64).init(allocator),
            .waiters = std.ArrayList(WaitRegistration).init(allocator),
            .next_connection_id = 0,
            .timeouts = timeouts,
        };
    }

    pub fn deinit(self: *AppHandler) void {
        var it_conn = self.connection_by_id.iterator();
        while (it_conn.next()) |entry| {
            entry.value_ptr.read_buffer.deinit();
            self.clearTransactionQueue(entry.value_ptr);
            entry.value_ptr.transaction_queue.deinit();
            self.cleanupStreamBlockingState(entry.value_ptr.blocking_stream_state);
        }
        var it = self.blocked_clients_by_key.iterator();
        while (it.next()) |entry| {
            while (entry.value_ptr.popFirst()) |node| {
                self.app_allocator.destroy(node);
            }
            self.app_allocator.free(entry.key_ptr.*);
        }
        self.blocked_clients_by_key.deinit();
        var stream_it = self.blocked_stream_clients_by_key.iterator();
        while (stream_it.next()) |entry| {
            while (entry.value_ptr.popFirst()) |node| {
                self.app_allocator.destroy(node);
            }
            self.app_allocator.free(entry.key_ptr.*);
        }
        self.blocked_stream_clients_by_key.deinit();
        self.replicas.deinit();
        self.replica_acks.deinit();
        self.waiters.deinit();

        if (self.master) |master| {
            self.app_allocator.free(master.host);
        }

        self.app_allocator.free(self.dir);
        self.app_allocator.free(self.db_filename);

        self.connection_by_id.deinit();
        self.timeouts.deinit();
    }

    pub fn registerMasterConnection(self: *AppHandler, allocator: std.mem.Allocator, connection_id: u64, listening_port: u16) ![]WriteOp {
        self.master_connection_id = connection_id;
        self.replication_offset = 0;

        var ops = std.ArrayList(WriteOp).init(allocator);
        errdefer ops.deinit();

        try ops.append(.{ .connection_id = connection_id, .bytes = try self.formatCommand(allocator, &[_][]const u8{"PING"}) });

        var port_buf: [16]u8 = undefined;
        const port_str = try std.fmt.bufPrint(&port_buf, "{d}", .{listening_port});
        try ops.append(.{ .connection_id = connection_id, .bytes = try self.formatCommand(allocator, &[_][]const u8{ "REPLCONF", "listening-port", port_str }) });

        try ops.append(.{ .connection_id = connection_id, .bytes = try self.formatCommand(allocator, &[_][]const u8{ "REPLCONF", "capa", "psync2" }) });

        try ops.append(.{ .connection_id = connection_id, .bytes = try self.formatCommand(allocator, &[_][]const u8{ "PSYNC", "?", "-1" }) });

        return ops.toOwnedSlice();
    }

    fn formatCommand(self: *AppHandler, allocator: std.mem.Allocator, parts: []const []const u8) ![]u8 {
        _ = self;
        var items = try allocator.alloc(Reply, parts.len);
        defer allocator.free(items);
        for (parts, 0..) |part, idx| {
            items[idx] = Reply{ .BulkString = part };
        }

        const command = Reply{ .Array = items };
        var buf = std.ArrayList(u8).init(allocator);
        defer buf.deinit();

        try format.writeReply(buf.writer(), command);
        return buf.toOwnedSlice();
    }

    pub fn recordPropagation(self: *AppHandler, bytes_len: usize) void {
        if (self.role != .master) return;
        if (self.replicas.items.len == 0) return;
        self.replication_offset += @intCast(bytes_len);
    }

    pub fn replicaAck(self: *AppHandler, replica_id: u64) u64 {
        return self.replica_acks.get(replica_id) orelse 0;
    }

    pub fn countReplicasAtOrBeyond(self: *AppHandler, target_offset: u64) usize {
        var count: usize = 0;
        for (self.replicas.items) |replica_id| {
            if (self.replicaAck(replica_id) >= target_offset) {
                count += 1;
            }
        }
        return count;
    }

    fn renderIntegerReply(allocator: std.mem.Allocator, value: usize) ![]u8 {
        var buf = std.ArrayList(u8).init(allocator);
        errdefer buf.deinit();
        try format.writeReply(buf.writer(), Reply{ .Integer = @intCast(value) });
        return try buf.toOwnedSlice();
    }

    fn finishWait(_: *AppHandler, conn: *ClientConnection) void {
        conn.wait_state = null;
        conn.deadline_us = null;
    }

    fn findWaiterIndex(self: *AppHandler, connection_id: u64) ?usize {
        var idx: usize = 0;
        while (idx < self.waiters.items.len) : (idx += 1) {
            if (self.waiters.items[idx].connection_id == connection_id) return idx;
        }
        return null;
    }

    fn removeWaiterAt(self: *AppHandler, idx: usize) void {
        const removed = self.waiters.swapRemove(idx);
        if (self.connection_by_id.getPtr(removed.connection_id)) |conn| {
            self.finishWait(conn);
        }
    }

    fn removeWaiterByConnection(self: *AppHandler, connection_id: u64) void {
        if (self.findWaiterIndex(connection_id)) |idx| {
            self.removeWaiterAt(idx);
        }
    }

    fn respondSatisfiedWaits(self: *AppHandler, allocator: std.mem.Allocator) !?[]WriteOp {
        var notifications = std.ArrayList(WriteOp).init(allocator);
        errdefer notifications.deinit();

        var idx: usize = self.waiters.items.len;
        while (idx > 0) {
            idx -= 1;
            const wait = self.waiters.items[idx];
            const acked = self.countReplicasAtOrBeyond(wait.target_offset);
            const total = self.replicas.items.len;
            if (@as(u64, @intCast(acked)) >= wait.required or acked == total) {
                const bytes = try renderIntegerReply(allocator, acked);
                try notifications.append(.{ .connection_id = wait.connection_id, .bytes = bytes });
                self.removeWaiterAt(idx);
            }
        }

        if (notifications.items.len == 0) return null;
        return try notifications.toOwnedSlice();
    }

    pub fn recordReplicaAck(
        self: *AppHandler,
        request_allocator: std.mem.Allocator,
        replica_id: u64,
        offset: u64,
    ) !?[]WriteOp {
        if (self.role != .master) return null;
        const current = self.replica_acks.get(replica_id) orelse 0;
        const updated = if (offset > current) offset else current;
        try self.replica_acks.put(replica_id, updated);
        return try self.respondSatisfiedWaits(request_allocator);
    }

    pub fn registerWait(
        self: *AppHandler,
        connection: *ClientConnection,
        required: u64,
        timeout_ms: u64,
        target_offset: u64,
    ) !void {
        connection.wait_state = .{
            .required = required,
            .target_offset = target_offset,
        };
        try self.waiters.append(.{
            .connection_id = connection.id,
            .required = required,
            .target_offset = target_offset,
        });

        if (timeout_ms > 0) {
            const now_us: i64 = std.time.microTimestamp();
            const deadline_us: i64 = now_us + @as(i64, @intCast(timeout_ms)) * @as(i64, std.time.us_per_ms);
            connection.deadline_us = deadline_us;
            try self.timeouts.add(.{ .deadline_us = deadline_us, .connection_id = connection.id });
        } else {
            connection.deadline_us = null;
        }
    }

    fn isMasterConnection(self: *AppHandler, connection_id: u64) bool {
        return self.master_connection_id != null and self.master_connection_id.? == connection_id;
    }

    fn updateReplicationOffset(
        self: *AppHandler,
        connection_id: u64,
        message_kind: StreamParser.MessageKind,
        consumed: usize,
    ) void {
        if (self.role != .replica) return;
        if (!self.isMasterConnection(connection_id)) return;

        if (message_kind == .Array) {
            self.replication_offset += @intCast(consumed);
        }
    }

    fn processParsedCommand(
        self: *AppHandler,
        conn: *ClientConnection,
        parts: [64]?[]const u8,
        request_allocator: std.mem.Allocator,
        notifications: *std.ArrayList(WriteOp),
    ) anyerror!bool {
        const single = try dispatcher.dispatchCommand(self, request_allocator, parts, conn);
        switch (single) {
            .Immediate => |value| {
                if (value.bytes.len != 0) {
                    try notifications.append(.{ .connection_id = conn.id, .bytes = value.bytes });
                }
                if (value.notify) |ns| try notifications.appendSlice(ns);
                return false;
            },
            .Blocked => |blk| {
                if (blk.notify) |ns| try notifications.appendSlice(ns);
                return true;
            },
        }
    }

    pub fn ingest(self: *AppHandler, connection_id: u64, incoming: []const u8, request_allocator: std.mem.Allocator) anyerror!IngestResult {
        const conn = self.connection_by_id.getPtr(connection_id) orelse return .Close;

        try conn.read_buffer.appendSlice(incoming);

        var notifications = std.ArrayList(WriteOp).init(request_allocator);

        while (true) {
            const available = conn.read_buffer.items[conn.read_start..];
            const outcome = conn.parser.parse(available);
            switch (outcome) {
                .NeedMore => break,
                .Error => return .Close,
                .Done => |done| {
                    const is_blocked = try self.processParsedCommand(conn, done.parts, request_allocator, &notifications);

                    self.updateReplicationOffset(connection_id, done.kind, done.consumed);

                    conn.read_start += done.consumed;
                    conn.parser.reset();

                    if (is_blocked) {
                        break;
                    }

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

    pub fn createConnection(self: *AppHandler) !u64 {
        const id = self.next_connection_id;
        self.next_connection_id += 1;

        try self.connection_by_id.put(id, ClientConnection{
            .id = id,
            .blocking_key = null,
            .blocking_node = null,
            .deadline_us = null,
            .blocking_stream_state = null,
            .wait_state = null,
            .in_transaction = false,
            .transaction_queue = std.ArrayList(TransactionCommand).init(self.app_allocator),
            .is_replica = false,
            .read_buffer = std.ArrayList(u8).init(self.app_allocator),
            .read_start = 0,
            .parser = StreamParser.init(),
        });

        return id;
    }

    pub fn removeConnection(self: *AppHandler, connection_id: u64) void {
        if (self.connection_by_id.fetchRemove(connection_id)) |removed_connection| {
            var conn = removed_connection.value;

            if (conn.wait_state != null) {
                self.removeWaiterByConnection(connection_id);
            }

            if (self.master_connection_id != null and self.master_connection_id.? == connection_id) {
                self.master_connection_id = null;
                self.replication_offset = 0;
            }

            if (conn.is_replica) {
                for (self.replicas.items, 0..) |r_id, i| {
                    if (r_id == connection_id) {
                        _ = self.replicas.orderedRemove(i);
                        break;
                    }
                }
                _ = self.replica_acks.remove(connection_id);
            }

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
            node_ptr.* = .{ .data = connection.id };
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
            try self.timeouts.add(.{ .deadline_us = deadline_us, .connection_id = connection.id });
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

    pub const DrainResult = struct {
        ops: []WriteOp,
        popped: usize,
    };

    pub fn drainWaitersForKey(self: *AppHandler, request_allocator: std.mem.Allocator, key: []const u8, pushed: usize) !DrainResult {
        const wait_list_ptr = self.blocked_clients_by_key.getPtr(key) orelse return .{ .ops = &.{}, .popped = 0 };

        var notifications = std.ArrayList(WriteOp).init(request_allocator);
        errdefer notifications.deinit();

        var actual_popped: usize = 0;

        for (0..pushed) |_| {
            const node_ptr = wait_list_ptr.popFirst() orelse break;

            const popped_values = try self.list_store.lpop(key, 1, request_allocator);
            if (popped_values.len == 0) {
                wait_list_ptr.prepend(node_ptr);
                break;
            }
            actual_popped += 1;

            const id = node_ptr.data;
            defer self.app_allocator.destroy(node_ptr);

            if (self.connection_by_id.getPtr(id)) |conn| {
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

            try notifications.append(.{ .connection_id = id, .bytes = bytes });

            const result = try self.ingest(id, &[_]u8{}, request_allocator);
            switch (result) {
                .Writes => |ops| try notifications.appendSlice(ops),
                .Blocked => {},
                .Close => {},
            }
        }

        return .{ .ops = try notifications.toOwnedSlice(), .popped = actual_popped };
    }

    pub fn expireDueWaiters(self: *AppHandler, now_us: i64, request_allocator: std.mem.Allocator) ![]WriteOp {
        var notifications = std.ArrayList(WriteOp).init(request_allocator);

        while (self.timeouts.peek()) |t| {
            if (t.deadline_us > now_us) break;

            const entry = self.timeouts.remove();
            const id = entry.connection_id;

            if (self.connection_by_id.getPtr(id)) |conn| {
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
                    try notifications.append(.{ .connection_id = id, .bytes = bytes });
                } else if (conn.blocking_stream_state != null) {
                    var buf = std.ArrayList(u8).init(request_allocator);
                    try format.writeReply(buf.writer(), Reply{ .Array = null });
                    const bytes = try buf.toOwnedSlice();
                    try notifications.append(.{ .connection_id = id, .bytes = bytes });
                    self.clearStreamBlockingState(conn);
                } else if (conn.wait_state) |_| {
                    if (self.findWaiterIndex(id)) |idx| {
                        const wait = self.waiters.items[idx];
                        const acked = self.countReplicasAtOrBeyond(wait.target_offset);
                        const bytes = try renderIntegerReply(request_allocator, acked);
                        try notifications.append(.{ .connection_id = id, .bytes = bytes });
                        self.removeWaiterAt(idx);
                    } else {
                        conn.wait_state = null;
                        conn.deadline_us = null;
                    }
                }

                const result = try self.ingest(id, &[_]u8{}, request_allocator);
                switch (result) {
                    .Writes => |ops| try notifications.appendSlice(ops),
                    .Blocked => {},
                    .Close => {},
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
