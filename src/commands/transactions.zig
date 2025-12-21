const std = @import("std");

const transactions = @import("../features/transactions.zig");
const TransactionCommand = transactions.TransactionCommand;
const connection_mod = @import("../network/connection.zig");
const Connection = connection_mod.Connection;
const resp_parser = @import("../network/resp/parser.zig");
const services_mod = @import("../services.zig");
const Services = services_mod.Services;
const WriteOp = services_mod.WriteOp;
const command_mod = @import("command.zig");
const Command = command_mod.Command;
const command_parser = @import("parser.zig");
const dispatcher = @import("dispatcher.zig");
const lists = @import("lists.zig");
const server = @import("server.zig");
const streams = @import("streams.zig");
const Output = dispatcher.Output;
const ErrorKind = @import("reply.zig").ErrorKind;
const Reply = @import("reply.zig").Reply;

pub fn handleMulti(services: *Services, connection: *Connection) Output {
    if (services.transactions.isInTransaction(connection.id)) {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.NestedMulti } } };
    }

    services.transactions.startTransaction(connection.id) catch {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.Syntax } } };
    };
    return .{ .reply = Reply{ .SimpleString = "OK" } };
}

pub fn handleDiscard(services: *Services, connection: *Connection) Output {
    if (!services.transactions.isInTransaction(connection.id)) {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.DiscardWithoutMulti } } };
    }

    services.transactions.endTransaction(connection.id);
    return .{ .reply = Reply{ .SimpleString = "OK" } };
}

pub fn handleExec(allocator: std.mem.Allocator, services: *Services, connection: *Connection) !Output {
    if (!services.transactions.isInTransaction(connection.id)) {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ExecWithoutMulti } } };
    }

    if (services.transactions.hasError(connection.id)) {
        services.transactions.endTransaction(connection.id);
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ExecAbort } } };
    }

    defer services.transactions.endTransaction(connection.id);

    const result = try executeQueuedTransaction(allocator, services, connection);
    const wrapped = try services.replication.wrapWithTransaction(allocator, result.side_effects);

    return .{ .reply = result.reply, .side_effects = wrapped };
}

fn dispatchExecCommand(
    allocator: std.mem.Allocator,
    services: *Services,
    connection: *Connection,
    kind: command_mod.CommandKind,
) !dispatcher.CommandOutcome {
    return switch (kind) {
        .Blpop => |data| .{ .Immediate = try lists.handleBlpopNonBlocking(allocator, services, data.key) },
        .Xread => |data| try streams.handleXreadNonBlocking(allocator, services, connection, data),
        .Wait => |data| try server.handleWait(allocator, services, connection, data.replicas_needed, 0),
        else => try dispatcher.dispatchCommand(services, allocator, kind, connection),
    };
}

fn executeQueuedTransaction(
    allocator: std.mem.Allocator,
    services: *Services,
    connection: *Connection,
) anyerror!Output {
    var client_replies = std.ArrayList(Reply).init(allocator);
    errdefer client_replies.deinit();

    var all_side_effects = std.ArrayList(WriteOp).init(allocator);
    errdefer all_side_effects.deinit();

    const queue = services.transactions.getQueue(connection.id) orelse {
        return .{ .reply = Reply{ .Array = &.{} } };
    };

    for (queue) |queued| {
        var parser = resp_parser.StreamParser.init(allocator);
        defer parser.deinit();

        const parse_result = parser.parse(queued.raw);
        const parts = switch (parse_result) {
            .Done => |done| done.parts,
            .NeedMore, .Error => {
                try client_replies.append(Reply{ .Error = .{ .kind = .Syntax } });
                continue;
            },
        };

        const parsed = try command_parser.parse(parts, queued.raw, allocator);
        switch (parsed) {
            .Error => |kind| try client_replies.append(Reply{ .Error = .{ .kind = kind } }),
            .Command => |cmd| {
                const outcome = try dispatchExecCommand(allocator, services, connection, cmd.kind);
                const result = switch (outcome) {
                    .Immediate => |r| r,
                    .Blocked => Output{},
                };

                if (result.reply) |reply| {
                    try client_replies.append(reply);
                } else {
                    try client_replies.append(Reply{ .BulkString = null });
                }

                const has_error = if (result.reply) |r| r == .Error else false;
                if (!has_error) {
                    const replica_ops = try services.replication.propagateToReplicas(allocator, &cmd);
                    if (replica_ops.len > 0) {
                        try all_side_effects.appendSlice(replica_ops);
                    }
                }

                try all_side_effects.appendSlice(result.side_effects);
            },
        }
    }

    return .{
        .reply = Reply{ .Array = try client_replies.toOwnedSlice() },
        .side_effects = try all_side_effects.toOwnedSlice(),
    };
}
