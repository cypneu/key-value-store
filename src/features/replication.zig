const std = @import("std");
const blocking = @import("blocking.zig");
const services = @import("../services.zig");
const reply_writer = @import("../network/resp/writer.zig");
const rdb_writer = @import("../storage/rdb/writer.zig");
const command_mod = @import("../commands/command.zig");
const StringStore = @import("../storage/string.zig").StringStore;
const ListStore = @import("../storage/list.zig").ListStore;
const StreamStore = @import("../storage/stream.zig").StreamStore;

pub const WriteOp = services.WriteOp;

pub const ServerRole = enum { master, replica };

pub const ReplicaConfig = struct {
    host: []const u8,
    port: u16,
};

pub const WaitRegistration = struct {
    connection_id: u64,
    required: u64,
    target_offset: u64,
};

pub const SnapshotRegistration = struct {
    connection_id: u64,
    read_fd: std.posix.fd_t,
    child_pid: std.posix.pid_t,
};

pub const ActiveSnapshot = struct {
    read_fd: std.posix.fd_t,
    child_pid: std.posix.pid_t,
    waiting_replicas: std.ArrayList(u64),

    fn init(allocator: std.mem.Allocator, read_fd: std.posix.fd_t, child_pid: std.posix.pid_t) ActiveSnapshot {
        return .{
            .read_fd = read_fd,
            .child_pid = child_pid,
            .waiting_replicas = std.ArrayList(u64).init(allocator),
        };
    }

    fn deinit(self: *ActiveSnapshot) void {
        self.waiting_replicas.deinit();
    }
};

pub const CleanupResult = struct {
    snapshot_orphaned: bool = false,
};

pub const ReplicaSnapshotData = struct {
    connection_id: u64,
    buffered_commands: [][]const u8,
};

pub const SnapshotTransferResult = struct {
    replicas: []ReplicaSnapshotData,
};

pub const WaitSatisfaction = struct {
    connection_id: u64,
    acked: usize,
};

pub const Propagation = struct {
    ops: []WriteOp,
    bytes_len: usize,
};

pub fn propagateCommand(
    allocator: std.mem.Allocator,
    replicas: []const u64,
    command_parts: []const []const u8,
) !Propagation {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    try buf.writer().print("*{d}\r\n", .{command_parts.len});
    for (command_parts) |part| {
        try buf.writer().print("${d}\r\n{s}\r\n", .{ part.len, part });
    }

    const command_bytes = try buf.toOwnedSlice();
    errdefer allocator.free(command_bytes);
    const bytes_len = command_bytes.len;

    if (replicas.len == 0) {
        allocator.free(command_bytes);
        return .{ .ops = &.{}, .bytes_len = bytes_len };
    }

    var ops = try allocator.alloc(WriteOp, replicas.len);
    errdefer allocator.free(ops);

    for (replicas, 0..) |replica_id, i| {
        ops[i] = .{
            .connection_id = replica_id,
            .reply = .{ .Bytes = try allocator.dupe(u8, command_bytes) },
        };
    }

    allocator.free(command_bytes);
    return .{ .ops = ops, .bytes_len = bytes_len };
}

fn propagateRaw(allocator: std.mem.Allocator, replicas: []const u64, raw: []const u8) !Propagation {
    const bytes_len = raw.len;

    if (replicas.len == 0) return .{ .ops = &.{}, .bytes_len = bytes_len };

    var ops = try allocator.alloc(WriteOp, replicas.len);
    errdefer allocator.free(ops);

    for (replicas, 0..) |replica_id, i| {
        ops[i] = .{
            .connection_id = replica_id,
            .reply = .{ .Bytes = try allocator.dupe(u8, raw) },
        };
    }

    return .{ .ops = ops, .bytes_len = bytes_len };
}

pub const ReplicaConnectionState = struct {
    is_loading_snapshot: bool = false,
    pending_replication: std.ArrayList([]const u8),
};

pub const ReplicationManager = struct {
    allocator: std.mem.Allocator,
    role: ServerRole,
    master: ?ReplicaConfig,
    master_connection_id: ?u64,
    offset: u64,
    replica_loading: bool,
    replicas: std.ArrayList(u64),
    acks: std.AutoHashMap(u64, u64),
    waiters: std.ArrayList(WaitRegistration),
    pending_snapshots: std.ArrayList(SnapshotRegistration),
    replica_states: std.AutoHashMap(u64, ReplicaConnectionState),
    active_snapshot: ?ActiveSnapshot,

    pub fn init(allocator: std.mem.Allocator, role: ServerRole, master: ?ReplicaConfig) ReplicationManager {
        return .{
            .allocator = allocator,
            .role = role,
            .master = master,
            .master_connection_id = null,
            .offset = 0,
            .replica_loading = role == .replica,
            .replicas = std.ArrayList(u64).init(allocator),
            .acks = std.AutoHashMap(u64, u64).init(allocator),
            .waiters = std.ArrayList(WaitRegistration).init(allocator),
            .pending_snapshots = std.ArrayList(SnapshotRegistration).init(allocator),
            .replica_states = std.AutoHashMap(u64, ReplicaConnectionState).init(allocator),
            .active_snapshot = null,
        };
    }

    pub fn deinit(self: *ReplicationManager) void {
        self.replicas.deinit();
        self.acks.deinit();
        self.waiters.deinit();
        self.pending_snapshots.deinit();
        var it = self.replica_states.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.pending_replication.items) |bytes| {
                self.allocator.free(bytes);
            }
            entry.value_ptr.pending_replication.deinit();
        }
        self.replica_states.deinit();
        if (self.active_snapshot) |*snap| {
            snap.deinit();
        }
    }

    pub fn isReplica(self: *ReplicationManager, connection_id: u64) bool {
        return self.replica_states.contains(connection_id);
    }

    pub fn markAsReplica(self: *ReplicationManager, connection_id: u64) !void {
        const gop = try self.replica_states.getOrPut(connection_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .pending_replication = std.ArrayList([]const u8).init(self.allocator),
            };
        }
    }

    fn isLoadingSnapshot(self: *ReplicationManager, connection_id: u64) bool {
        const state = self.replica_states.getPtr(connection_id) orelse return false;
        return state.is_loading_snapshot;
    }

    pub fn setLoadingSnapshot(self: *ReplicationManager, connection_id: u64, loading: bool) void {
        if (self.replica_states.getPtr(connection_id)) |state| {
            state.is_loading_snapshot = loading;
        }
    }

    pub fn prepareSnapshot(
        self: *ReplicationManager,
        string_store: *StringStore,
        list_store: *ListStore,
        stream_store: *StreamStore,
        connection_id: u64,
    ) !?SnapshotRegistration {
        if (self.active_snapshot) |*snap| {
            try snap.waiting_replicas.append(connection_id);
            self.setLoadingSnapshot(connection_id, true);
            return null;
        }

        self.setLoadingSnapshot(connection_id, true);

        const pipe = try rdb_writer.forkSnapshotPipeNonBlocking(string_store, list_store, stream_store);

        self.active_snapshot = ActiveSnapshot.init(self.allocator, pipe.read_fd, pipe.child_pid);
        try self.active_snapshot.?.waiting_replicas.append(connection_id);

        return .{
            .connection_id = connection_id,
            .read_fd = pipe.read_fd,
            .child_pid = pipe.child_pid,
        };
    }

    pub fn completeSnapshotTransfer(self: *ReplicationManager, allocator: std.mem.Allocator) !SnapshotTransferResult {
        const waiting_replicas = self.completeActiveSnapshot();
        defer self.allocator.free(waiting_replicas);

        var replicas = try allocator.alloc(ReplicaSnapshotData, waiting_replicas.len);
        errdefer allocator.free(replicas);

        for (waiting_replicas, 0..) |conn_id, idx| {
            self.setLoadingSnapshot(conn_id, false);
            const buffered = try self.takeBufferedReplication(conn_id, allocator);
            replicas[idx] = .{
                .connection_id = conn_id,
                .buffered_commands = buffered,
            };
        }

        return .{ .replicas = replicas };
    }

    fn completeActiveSnapshot(self: *ReplicationManager) []u64 {
        if (self.active_snapshot) |*snap| {
            const replicas = snap.waiting_replicas.toOwnedSlice() catch {
                snap.deinit();
                self.active_snapshot = null;
                return self.allocator.alloc(u64, 0) catch @as([]u64, &.{});
            };
            snap.deinit();
            self.active_snapshot = null;
            return replicas;
        }
        return self.allocator.alloc(u64, 0) catch @as([]u64, &.{});
    }

    pub fn propagateToReplicas(
        self: *ReplicationManager,
        allocator: std.mem.Allocator,
        command: *const command_mod.Command,
    ) ![]WriteOp {
        if (command_mod.isTransactionControl(command.tag())) return &.{};
        if (!command_mod.isReplicationCommand(command.tag())) return &.{};

        const prop = try propagateRaw(allocator, self.replicas.items, command.raw);
        defer allocator.free(prop.ops);

        self.recordOffset(prop.bytes_len);

        if (self.replicas.items.len == 0) return &.{};

        var result = std.ArrayList(WriteOp).init(allocator);
        errdefer result.deinit();

        for (prop.ops) |op| {
            if (self.isLoadingSnapshot(op.connection_id)) {
                const bytes = try reply_writer.renderReply(allocator, op.reply);
                try self.bufferReplicaWrite(op.connection_id, bytes);
            } else {
                try result.append(op);
            }
        }

        return try result.toOwnedSlice();
    }

    pub fn wrapWithTransaction(self: *ReplicationManager, allocator: std.mem.Allocator, ops: []WriteOp) ![]WriteOp {
        if (self.replicas.items.len == 0) return ops;

        var final = std.ArrayList(WriteOp).init(allocator);
        errdefer final.deinit();

        const multi_args = [_][]const u8{"MULTI"};
        const multi = try propagateCommand(allocator, self.replicas.items, multi_args[0..]);
        self.recordOffset(multi.bytes_len);

        for (multi.ops) |op| {
            if (self.isLoadingSnapshot(op.connection_id)) {
                const bytes = try reply_writer.renderReply(allocator, op.reply);
                self.bufferReplicaWrite(op.connection_id, bytes) catch {};
            } else {
                try final.append(op);
            }
        }

        try final.appendSlice(ops);

        const exec_args = [_][]const u8{"EXEC"};
        const exec = try propagateCommand(allocator, self.replicas.items, exec_args[0..]);
        self.recordOffset(exec.bytes_len);

        for (exec.ops) |op| {
            if (self.isLoadingSnapshot(op.connection_id)) {
                const bytes = try reply_writer.renderReply(allocator, op.reply);
                self.bufferReplicaWrite(op.connection_id, bytes) catch {};
            } else {
                try final.append(op);
            }
        }

        return try final.toOwnedSlice();
    }

    fn formatCommandBytes(allocator: std.mem.Allocator, parts: []const []const u8) ![]u8 {
        var buf = std.ArrayList(u8).init(allocator);
        errdefer buf.deinit();

        try buf.writer().print("*{d}\r\n", .{parts.len});
        for (parts) |part| {
            try buf.writer().print("${d}\r\n{s}\r\n", .{ part.len, part });
        }

        return try buf.toOwnedSlice();
    }

    pub fn registerMasterConnection(self: *ReplicationManager, allocator: std.mem.Allocator, connection_id: u64, listening_port: u16) ![]WriteOp {
        self.master_connection_id = connection_id;
        self.offset = 0;

        var ops = std.ArrayList(WriteOp).init(allocator);
        errdefer ops.deinit();

        try ops.append(.{ .connection_id = connection_id, .reply = .{ .Bytes = try formatCommandBytes(allocator, &[_][]const u8{"PING"}) } });

        var port_buf: [16]u8 = undefined;
        const port_str = try std.fmt.bufPrint(&port_buf, "{d}", .{listening_port});
        try ops.append(.{ .connection_id = connection_id, .reply = .{ .Bytes = try formatCommandBytes(allocator, &[_][]const u8{ "REPLCONF", "listening-port", port_str }) } });

        try ops.append(.{ .connection_id = connection_id, .reply = .{ .Bytes = try formatCommandBytes(allocator, &[_][]const u8{ "REPLCONF", "capa", "psync2" }) } });

        try ops.append(.{ .connection_id = connection_id, .reply = .{ .Bytes = try formatCommandBytes(allocator, &[_][]const u8{ "PSYNC", "?", "-1" }) } });

        return ops.toOwnedSlice();
    }

    pub fn applyFullResyncMessage(self: *ReplicationManager, message: []const u8) void {
        if (!std.mem.startsWith(u8, message, "FULLRESYNC")) return;

        var it = std.mem.tokenizeScalar(u8, message, ' ');
        _ = it.next(); // FULLRESYNC
        _ = it.next(); // replid
        if (it.next()) |offset_slice| {
            self.offset = std.fmt.parseInt(u64, offset_slice, 10) catch 0;
        }
        self.setReplicaLoading(true);
    }

    pub fn isReplicaLoading(self: *ReplicationManager) bool {
        return self.role == .replica and self.replica_loading;
    }

    pub fn setReplicaLoading(self: *ReplicationManager, loading: bool) void {
        if (self.role != .replica) return;
        self.replica_loading = loading;
    }

    pub fn recordIncomingMasterMessage(self: *ReplicationManager, connection_id: u64, counts_towards_offset: bool, consumed: usize) void {
        if (self.role != .replica) return;
        if (!self.isMasterConnection(connection_id)) return;
        if (!counts_towards_offset) return;
        self.offset += @intCast(consumed);
    }

    pub fn isMasterConnection(self: *ReplicationManager, connection_id: u64) bool {
        return self.master_connection_id != null and self.master_connection_id.? == connection_id;
    }

    pub fn clearMasterConnection(self: *ReplicationManager, connection_id: u64) void {
        if (self.isMasterConnection(connection_id)) {
            self.master_connection_id = null;
            self.offset = 0;
            self.setReplicaLoading(false);
        }
    }

    pub fn registerReplica(self: *ReplicationManager, connection_id: u64) !void {
        try self.replicas.append(connection_id);
    }

    fn recordReplicaAck(self: *ReplicationManager, allocator: std.mem.Allocator, replica_id: u64, offset: u64) !?[]WaitSatisfaction {
        if (self.role != .master) return null;
        const current = self.acks.get(replica_id) orelse 0;
        const updated = if (offset > current) offset else current;
        try self.acks.put(replica_id, updated);
        return try self.takeSatisfiedWaiters(allocator);
    }

    pub fn recordReplicaAckAndBuildNotifications(
        self: *ReplicationManager,
        blocking_mgr: *blocking.BlockingManager,
        allocator: std.mem.Allocator,
        replica_id: u64,
        offset: u64,
    ) !?[]WriteOp {
        const satisfied = try self.recordReplicaAck(allocator, replica_id, offset) orelse return null;
        defer allocator.free(satisfied);

        var notifications = std.ArrayList(WriteOp).init(allocator);
        errdefer notifications.deinit();

        for (satisfied) |wait| {
            _ = blocking_mgr.blocked_clients.remove(wait.connection_id);
            const bytes = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{wait.acked});
            try notifications.append(.{ .connection_id = wait.connection_id, .reply = .{ .Bytes = bytes } });
        }

        return try notifications.toOwnedSlice();
    }

    pub fn getAck(self: *ReplicationManager, replica_id: u64) u64 {
        return self.acks.get(replica_id) orelse 0;
    }

    fn countReplicasAtOrAbove(self: *ReplicationManager, target_offset: u64) usize {
        var count: usize = 0;
        for (self.replicas.items) |replica_id| {
            if (self.getAck(replica_id) >= target_offset) {
                count += 1;
            }
        }
        return count;
    }

    pub fn recordOffset(self: *ReplicationManager, bytes_len: usize) void {
        if (self.role != .master) return;
        self.offset += @intCast(bytes_len);
    }

    pub fn registerWait(
        self: *ReplicationManager,
        blocking_mgr: *blocking.BlockingManager,
        connection_id: u64,
        required: u64,
        timeout_ms: u64,
        target_offset: u64,
    ) !void {
        try blocking_mgr.registerWaitBlockingClient(connection_id, .{
            .required = required,
            .target_offset = target_offset,
        }, timeout_ms);
        try self.waiters.append(.{
            .connection_id = connection_id,
            .required = required,
            .target_offset = target_offset,
        });
    }

    pub fn removeWaiterByConnection(self: *ReplicationManager, connection_id: u64) void {
        if (self.findWaiterIndex(connection_id)) |idx| {
            _ = self.waiters.swapRemove(idx);
        }
    }

    fn findWaiterIndex(self: *ReplicationManager, connection_id: u64) ?usize {
        for (self.waiters.items, 0..) |w, i| {
            if (w.connection_id == connection_id) return i;
        }
        return null;
    }

    fn takeSatisfiedWaiters(self: *ReplicationManager, allocator: std.mem.Allocator) !?[]WaitSatisfaction {
        var satisfied = std.ArrayList(WaitSatisfaction).init(allocator);
        errdefer satisfied.deinit();

        var idx: usize = self.waiters.items.len;
        while (idx > 0) {
            idx -= 1;
            const wait = self.waiters.items[idx];
            const acked = self.countReplicasAtOrAbove(wait.target_offset);
            if (@as(u64, @intCast(acked)) >= wait.required) {
                _ = self.waiters.swapRemove(idx);
                try satisfied.append(.{ .connection_id = wait.connection_id, .acked = acked });
            }
        }

        if (satisfied.items.len == 0) return null;
        return try satisfied.toOwnedSlice();
    }

    pub fn expireWaiter(self: *ReplicationManager, connection_id: u64) ?usize {
        const idx = self.findWaiterIndex(connection_id) orelse return null;
        const wait = self.waiters.items[idx];
        const acked = self.countReplicasAtOrAbove(wait.target_offset);
        _ = self.waiters.swapRemove(idx);
        return acked;
    }

    fn bufferReplicaWrite(self: *ReplicationManager, connection_id: u64, bytes: []const u8) !void {
        const state = self.replica_states.getPtr(connection_id) orelse return;
        if (!state.is_loading_snapshot) return;
        const copy = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(copy);
        try state.pending_replication.append(copy);
    }

    fn takeBufferedReplication(self: *ReplicationManager, connection_id: u64, allocator: std.mem.Allocator) ![][]const u8 {
        const state = self.replica_states.getPtr(connection_id) orelse return &[_][]const u8{};
        const count = state.pending_replication.items.len;
        if (count == 0) return &[_][]const u8{};

        const out = try allocator.alloc([]const u8, count);
        for (state.pending_replication.items, 0..) |b, idx| {
            out[idx] = b;
        }
        state.pending_replication.clearRetainingCapacity();
        return out;
    }

    pub fn cleanupReplicaState(self: *ReplicationManager, connection_id: u64) CleanupResult {
        var result = CleanupResult{};

        if (self.active_snapshot) |*snap| {
            for (snap.waiting_replicas.items, 0..) |replica_id, idx| {
                if (replica_id == connection_id) {
                    _ = snap.waiting_replicas.orderedRemove(idx);
                    break;
                }
            }

            if (snap.waiting_replicas.items.len == 0) {
                result.snapshot_orphaned = true;
                snap.deinit();
                self.active_snapshot = null;
            }
        }

        if (self.replica_states.fetchRemove(connection_id)) |kv| {
            for (kv.value.pending_replication.items) |bytes| {
                self.allocator.free(bytes);
            }
            var pending = kv.value.pending_replication;
            pending.deinit();
        }

        for (self.replicas.items, 0..) |replica_id, idx| {
            if (replica_id == connection_id) {
                _ = self.replicas.swapRemove(idx);
                break;
            }
        }

        _ = self.acks.remove(connection_id);

        return result;
    }
};
