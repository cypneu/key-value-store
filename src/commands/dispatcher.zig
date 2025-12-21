const std = @import("std");

const replication = @import("../features/replication.zig");
const SnapshotRegistration = replication.SnapshotRegistration;
const connection = @import("../network/connection.zig");
const Connection = connection.Connection;
const services_mod = @import("../services.zig");
const Services = services_mod.Services;
const WriteOp = services_mod.WriteOp;
const command_mod = @import("command.zig");
const CommandKind = command_mod.CommandKind;
const lists = @import("lists.zig");
const reply_mod = @import("reply.zig");
const Reply = reply_mod.Reply;
const server = @import("server.zig");
const streams = @import("streams.zig");
const strings = @import("strings.zig");
const transactions = @import("transactions.zig");

const BlockedOutcome = struct { replies: []WriteOp = &.{} };

pub const Output = struct {
    reply: ?Reply = null,
    side_effects: []WriteOp = &.{},
    snapshot: ?SnapshotRegistration = null,
};

pub const CommandOutcome = union(enum) {
    Immediate: Output,
    Blocked: BlockedOutcome,
};

pub fn dispatchCommand(
    services: *Services,
    allocator: std.mem.Allocator,
    command: CommandKind,
    conn: *Connection,
) !CommandOutcome {
    return switch (command) {
        // Blocking commands
        .Blpop => |data| try lists.handleBlpop(allocator, services, conn, data.key, data.timeout_secs),
        .Xread => |data| try streams.handleXread(allocator, services, conn, data),
        .Wait => |data| try server.handleWait(allocator, services, conn, data.replicas_needed, data.timeout_ms),

        // Transaction control
        .Multi => .{ .Immediate = transactions.handleMulti(services, conn) },
        .Discard => .{ .Immediate = transactions.handleDiscard(services, conn) },
        .Exec => .{ .Immediate = try transactions.handleExec(allocator, services, conn) },

        // Simple commands
        .Ping => .{ .Immediate = .{ .reply = .{ .SimpleString = "PONG" } } },
        .Echo => |data| .{ .Immediate = .{ .reply = try strings.handleEcho(data.value) } },
        .Get => |data| .{ .Immediate = .{ .reply = try strings.handleGet(services, data.key) } },
        .Type => |data| .{ .Immediate = .{ .reply = try strings.handleType(services, data.key) } },
        .Set => |data| .{ .Immediate = .{ .reply = try strings.handleSet(services, data.key, data.value, data.px_ms) } },
        .Incr => |data| .{ .Immediate = .{ .reply = try strings.handleIncr(services, data.key) } },
        .Info => .{ .Immediate = .{ .reply = try server.handleInfo(allocator, services) } },
        .Lrange => |data| .{ .Immediate = .{ .reply = try lists.handleLrange(allocator, services, data.key, data.start, data.end) } },
        .Llen => |data| .{ .Immediate = .{ .reply = try lists.handleLlen(services, data.key) } },
        .Lpop => |data| .{ .Immediate = .{ .reply = try lists.handleLpop(allocator, services, data.key, data.count) } },
        .Xrange => |data| .{ .Immediate = .{ .reply = try streams.handleXrange(allocator, services, data.key, data.start, data.end) } },
        .ConfigGet => |data| .{ .Immediate = .{ .reply = try server.handleConfig(allocator, services, data.param) } },
        .Keys => .{ .Immediate = .{ .reply = try server.handleKeys(allocator, services) } },
        .Save => .{ .Immediate = .{ .reply = try server.handleSave(allocator, services) } },

        // Commands with notifications
        .Lpush => |data| .{ .Immediate = try lists.handleLpush(allocator, services, data.key, data.values) },
        .Rpush => |data| .{ .Immediate = try lists.handleRpush(allocator, services, data.key, data.values) },
        .Xadd => |data| .{ .Immediate = try streams.handleXadd(allocator, services, data.key, data.entry_id, data.fields) },

        // Replication
        .Replconf => |data| .{ .Immediate = try server.handleReplconf(allocator, services, conn.id, data) },
        .Psync => .{ .Immediate = try server.handlePsync(allocator, services, conn) },
    };
}
