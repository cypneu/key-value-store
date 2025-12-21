const std = @import("std");

const command_mod = @import("commands/command.zig");
const dispatcher = @import("commands/dispatcher.zig");
const CommandOutcome = dispatcher.CommandOutcome;
const command_parser = @import("commands/parser.zig");
const reply = @import("commands/reply.zig");
const Reply = reply.Reply;
const command_transactions = @import("commands/transactions.zig");
const config = @import("config.zig");
const blocking = @import("features/blocking.zig");
const replication = @import("features/replication.zig");
const SnapshotRegistration = replication.SnapshotRegistration;
const transactions = @import("features/transactions.zig");
const connection = @import("network/connection.zig");
const Connection = connection.Connection;
const ListStore = @import("storage/list.zig").ListStore;
const rdb_loader = @import("storage/rdb/loader.zig");
const StreamStore = @import("storage/stream.zig").StreamStore;
const StringStore = @import("storage/string.zig").StringStore;

pub const WriteOp = struct {
    connection_id: u64,
    reply: Reply,
};

const ProcessResult = struct {
    writes: []WriteOp = &.{},
    snapshot: ?SnapshotRegistration = null,
};

const log = std.log.scoped(.services);

pub const Services = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: config.Config,

    string_store: StringStore,
    list_store: ListStore,
    stream_store: StreamStore,

    blocking: blocking.BlockingManager,
    replication: replication.ReplicationManager,
    transactions: transactions.TransactionManager,

    connections: connection.ConnectionRegistry,

    pub fn init(cfg: config.Config) Services {
        return .{
            .allocator = cfg.allocator,
            .config = cfg,
            .string_store = StringStore.init(cfg.allocator),
            .list_store = ListStore.init(cfg.allocator),
            .stream_store = StreamStore.init(cfg.allocator),
            .blocking = blocking.BlockingManager.init(cfg.allocator),
            .replication = replication.ReplicationManager.init(cfg.allocator, cfg.role, cfg.replica),
            .transactions = transactions.TransactionManager.init(cfg.allocator),
            .connections = connection.ConnectionRegistry.init(cfg.allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it_conn = self.connections.iterator();
        while (it_conn.next()) |entry| {
            self.blocking.clearStreamState(entry.value_ptr.id);
        }
        self.connections.deinit();
        self.transactions.deinit();
        self.replication.deinit();
        self.blocking.deinit();
        self.stream_store.deinit();
        self.list_store.deinit();
        self.string_store.deinit();
    }

    pub fn processBytes(
        self: *Self,
        connection_id: u64,
        incoming: []const u8,
        request_allocator: std.mem.Allocator,
    ) !ProcessResult {
        var notifs = std.ArrayList(WriteOp).init(request_allocator);
        return try self.ingest(connection_id, incoming, request_allocator, &notifs);
    }

    pub fn ingest(
        self: *Self,
        connection_id: u64,
        incoming: []const u8,
        allocator: std.mem.Allocator,
        notifs: *std.ArrayList(WriteOp),
    ) anyerror!ProcessResult {
        const conn = self.connections.get(connection_id) orelse return .{};
        try conn.feedBytes(incoming);

        var result_snapshot: ?SnapshotRegistration = null;

        while (conn.nextFrame()) |frame| {
            const outcome = try self.processFrame(conn, frame, allocator);

            self.replication.recordIncomingMasterMessage(connection_id, frame.type == .command, frame.consumed);
            conn.consume(frame.consumed);

            switch (outcome) {
                .Immediate => |imm| {
                    if (imm.reply) |r| try notifs.append(.{ .connection_id = conn.id, .reply = r });
                    try notifs.appendSlice(imm.side_effects);
                    result_snapshot = imm.snapshot;
                },
                .Blocked => |blk| {
                    try notifs.appendSlice(blk.replies);
                    return .{ .writes = notifs.items, .snapshot = result_snapshot };
                },
            }
        }

        if (conn.parse_error) {
            return error.ProtocolError;
        }

        return .{ .writes = notifs.items, .snapshot = result_snapshot };
    }

    pub fn tick(self: *Self, now_us: i64, alloc: std.mem.Allocator) ![]WriteOp {
        return self.expireDueWaiters(now_us, alloc);
    }

    pub fn drainWaitersForKey(self: *Self, allocator: std.mem.Allocator, key: []const u8, pushed: usize) !blocking.DrainResult {
        const wait_list_ptr = self.blocking.blocked_clients_by_key.getPtr(key) orelse return .{ .ops = &.{}, .popped = 0 };

        var notifications = std.ArrayList(WriteOp).init(allocator);
        errdefer notifications.deinit();

        var actual_popped: usize = 0;

        for (0..pushed) |_| {
            const node_ptr = wait_list_ptr.popFirst() orelse break;

            const popped_values = try self.list_store.lpop(key, 1, allocator);
            if (popped_values.len == 0) {
                wait_list_ptr.prepend(node_ptr);
                break;
            }
            actual_popped += 1;

            const id = node_ptr.data;
            defer self.blocking.allocator.destroy(node_ptr);

            _ = self.blocking.blocked_clients.remove(id);

            var replies = try allocator.alloc(Reply, 2);
            const key_copy = try allocator.dupe(u8, key);
            replies[0] = .{ .BulkString = key_copy };
            replies[1] = .{ .BulkString = popped_values[0] };

            try notifications.append(.{ .connection_id = id, .reply = Reply{ .Array = replies } });

            _ = try self.ingest(id, &[_]u8{}, allocator, &notifications);
        }

        return .{ .ops = try notifications.toOwnedSlice(), .popped = actual_popped };
    }

    fn expireDueWaiters(self: *Self, now_us: i64, allocator: std.mem.Allocator) ![]WriteOp {
        var notifications = std.ArrayList(WriteOp).init(allocator);
        errdefer notifications.deinit();

        while (self.blocking.timeouts.peek()) |t| {
            if (t.deadline_us > now_us) break;

            const entry = self.blocking.timeouts.remove();
            const id = entry.connection_id;

            const maybe_state = self.blocking.blocked_clients.get(id);
            if (maybe_state) |state| {
                switch (state) {
                    .List => |list| {
                        self.blocking.removeBlockedClientNode(list.key, list.node);
                        _ = self.blocking.blocked_clients.remove(id);
                        try notifications.append(.{ .connection_id = id, .reply = .{ .Array = null } });
                    },
                    .Stream => |stream| {
                        self.blocking.cleanupStreamBlockingState(stream);
                        _ = self.blocking.blocked_clients.remove(id);
                        try notifications.append(.{ .connection_id = id, .reply = .{ .Array = null } });
                    },
                    .Wait => {
                        if (self.replication.expireWaiter(id)) |acked| {
                            try notifications.append(.{ .connection_id = id, .reply = .{ .Integer = @intCast(acked) } });
                        }
                        _ = self.blocking.blocked_clients.remove(id);
                    },
                }
                _ = try self.ingest(id, &[_]u8{}, allocator, &notifications);
            }
        }

        return try notifications.toOwnedSlice();
    }

    pub fn removeConnection(self: *Self, connection_id: u64) replication.CleanupResult {
        if (self.blocking.blocked_clients.get(connection_id)) |state| {
            switch (state) {
                .List => |list| self.blocking.removeBlockedClientNode(list.key, list.node),
                .Stream => |stream| self.blocking.cleanupStreamBlockingState(stream),
                .Wait => self.replication.removeWaiterByConnection(connection_id),
            }
            _ = self.blocking.blocked_clients.remove(connection_id);
        }

        self.transactions.endTransaction(connection_id);

        const result = self.replication.cleanupReplicaState(connection_id);

        if (self.replication.isMasterConnection(connection_id)) {
            self.replication.clearMasterConnection(connection_id);
        }

        if (self.connections.remove(connection_id)) |removed_conn| {
            var conn = removed_conn;
            conn.deinit();
        }

        return result;
    }

    fn processFrame(
        self: *Self,
        conn: *Connection,
        frame: connection.Frame,
        allocator: std.mem.Allocator,
    ) !CommandOutcome {
        if (frame.type == .command) {
            if (self.replication.isReplicaLoading() and !self.replication.isMasterConnection(conn.id)) {
                return .{ .Immediate = .{ .reply = Reply{ .Error = .{ .kind = reply.ErrorKind.Loading } } } };
            }
            const parsed = try command_parser.parse(frame.parts, frame.raw, allocator);
            const outcome: CommandOutcome = switch (parsed) {
                .Command => |cmd| try self.processCommand(conn, &cmd, allocator),
                .Error => |kind| blk: {
                    if (self.transactions.isInTransaction(conn.id)) {
                        self.transactions.markError(conn.id);
                    }
                    break :blk .{ .Immediate = .{ .reply = .{ .Error = .{ .kind = kind } } } };
                },
            };
            return self.suppressReplyIfFromMaster(outcome, conn.id, if (parsed == .Command) parsed.Command.kind else null);
        }

        if (frame.type == .fullresync) self.replication.applyFullResyncMessage(frame.parts[0]);
        if (frame.type == .rdb_snapshot) {
            self.string_store.clear();
            self.list_store.clear();
            self.stream_store.clear();
            try rdb_loader.loadRDBFromBytes(allocator, frame.parts[0], self);
            self.replication.setReplicaLoading(false);
            return .{ .Immediate = .{} };
        }
        return .{ .Immediate = .{} };
    }

    fn processCommand(
        self: *Self,
        conn: *Connection,
        command: *const command_mod.Command,
        allocator: std.mem.Allocator,
    ) !CommandOutcome {
        if (self.transactions.isInTransaction(conn.id) and !command_mod.isTransactionControl(command.tag())) {
            try self.transactions.queueCommand(conn.id, command.raw);
            return .{ .Immediate = .{ .reply = Reply{ .SimpleString = "QUEUED" } } };
        }

        var out = try dispatcher.dispatchCommand(self, allocator, command.kind, conn);
        if (out != .Immediate) return out;
        if (out.Immediate.reply) |r| if (r == .Error) return out;

        const replica_ops = try self.replication.propagateToReplicas(allocator, command);
        if (replica_ops.len > 0) {
            out.Immediate.side_effects = try std.mem.concat(allocator, WriteOp, &.{ replica_ops, out.Immediate.side_effects });
        }

        return out;
    }

    fn suppressReplyIfFromMaster(self: *Self, outcome: CommandOutcome, conn_id: u64, kind: ?command_mod.CommandKind) CommandOutcome {
        if (!self.replication.isMasterConnection(conn_id)) return outcome;
        if (kind != null and command_mod.isReplconfGetack(kind.?)) return outcome;

        var result = outcome;
        if (result == .Immediate) result.Immediate.reply = null;
        return result;
    }
};
