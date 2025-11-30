const std = @import("std");
const Command = @import("command.zig").Command;
const handlers = @import("handlers.zig");
const AppHandler = @import("state.zig").AppHandler;
const ClientConnection = @import("state.zig").ClientConnection;
const format = @import("resp/format.zig");

const Reply = @import("reply.zig").Reply;
const ErrorKind = @import("reply.zig").ErrorKind;
const WriteOp = @import("state.zig").WriteOp;
const replication = @import("replication.zig");

pub const ProcessResult = union(enum) {
    Immediate: struct { bytes: []const u8, notify: ?[]WriteOp },
    Blocked: struct { notify: ?[]WriteOp },
};

const ImmediateCommandResult = struct {
    reply: Reply,
    notify: ?[]WriteOp,
};

const CommandResult = struct {
    reply: Reply,
    notify: []WriteOp,
};

pub fn dispatchCommand(handler: *AppHandler, request_allocator: std.mem.Allocator, command_parts: [64]?[]const u8, client_connection: *ClientConnection) !ProcessResult {
    const is_from_master = if (handler.master_connection_id) |mid| mid == client_connection.id else false;

    const command_str = command_parts[0] orelse return emptyImmediate();
    const command = Command.fromSlice(command_str) orelse {
        if (is_from_master) {
            return emptyImmediate();
        }
        const bytes = try renderReply(request_allocator, Reply{ .Error = .{ .kind = ErrorKind.UnknownCommand } });
        return makeImmediate(bytes, null);
    };

    if (command == .REPLCONF and command_parts[1] != null and std.ascii.eqlIgnoreCase(command_parts[1].?, "ACK")) {
        const offset = std.fmt.parseInt(u64, command_parts[2] orelse "0", 10) catch 0;
        const notify = try handler.recordReplicaAck(request_allocator, client_connection.id, offset);
        return ProcessResult{ .Immediate = .{ .bytes = &[_]u8{}, .notify = notify } };
    }

    const result = switch (command) {
        .BLPOP => blk: {
            const blpop_result = try handlers.handleBlpop(request_allocator, handler, client_connection, command_parts);
            break :blk switch (blpop_result) {
                .Value => |outcome| makeImmediate(try renderReply(request_allocator, outcome.reply), outcome.notify),
                .Blocked => ProcessResult{ .Blocked = .{ .notify = null } },
            };
        },
        .XREAD => blk: {
            const xread_result = try handlers.handleXread(request_allocator, handler, client_connection, command_parts);
            break :blk switch (xread_result) {
                .Value => |reply| makeImmediate(try renderReply(request_allocator, reply), null),
                .Blocked => ProcessResult{ .Blocked = .{ .notify = null } },
            };
        },
        .WAIT => blk: {
            const wait_result = try handlers.handleWait(request_allocator, handler, client_connection, command_parts);
            break :blk switch (wait_result) {
                .Value => |reply| makeImmediate(try renderReply(request_allocator, reply), null),
                .Blocked => |blocked| ProcessResult{ .Blocked = .{ .notify = blocked.notify } },
            };
        },
        else => blk: {
            const immediate = try dispatchImmediateCommand(request_allocator, handler, client_connection, command, command_parts);
            const bytes = try renderReply(request_allocator, immediate.reply);
            break :blk makeImmediate(bytes, immediate.notify);
        },
    };

    if (is_from_master) {
        var allow_reply = false;
        if (command == .REPLCONF and command_parts[1] != null) {
            if (std.ascii.eqlIgnoreCase(command_parts[1].?, "GETACK")) {
                allow_reply = true;
            }
        }

        if (!allow_reply) {
            switch (result) {
                .Immediate => |imm| {
                    return ProcessResult{ .Immediate = .{ .bytes = &[_]u8{}, .notify = imm.notify } };
                },
                .Blocked => |blk| return ProcessResult{ .Blocked = .{ .notify = blk.notify } },
            }
        }
    }

    return result;
}

fn executeCommand(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    client_connection: *ClientConnection,
    command: Command,
    command_parts: [64]?[]const u8,
) !CommandResult {
    return switch (command) {
        .PING => .{ .reply = Reply{ .SimpleString = "PONG" }, .notify = &.{} },
        .ECHO => .{ .reply = try handlers.handleEcho(command_parts), .notify = &.{} },
        .GET => .{ .reply = try handlers.handleGet(handler, command_parts), .notify = &.{} },
        .TYPE => .{ .reply = try handlers.handleType(handler, command_parts), .notify = &.{} },
        .SET => .{ .reply = try handlers.handleSet(handler, command_parts), .notify = &.{} },
        .INCR => .{ .reply = try handlers.handleIncr(handler, command_parts), .notify = &.{} },
        .INFO => .{ .reply = try handlers.handleInfo(handler, command_parts), .notify = &.{} },
        .LPUSH => blk: {
            const out = try handlers.handleLpush(allocator, handler, command_parts);
            break :blk .{ .reply = out.reply, .notify = out.notify };
        },
        .RPUSH => blk: {
            const out = try handlers.handleRpush(allocator, handler, command_parts);
            break :blk .{ .reply = out.reply, .notify = out.notify };
        },
        .LRANGE => .{ .reply = try handlers.handleLrange(allocator, handler, command_parts), .notify = &.{} },
        .LLEN => .{ .reply = try handlers.handleLlen(handler, command_parts), .notify = &.{} },
        .LPOP => .{ .reply = try handlers.handleLpop(allocator, handler, command_parts), .notify = &.{} },
        .XADD => blk: {
            const out = try handlers.handleXadd(allocator, handler, command_parts);
            break :blk .{ .reply = out.reply, .notify = out.notify };
        },
        .XRANGE => .{ .reply = try handlers.handleXrange(allocator, handler, command_parts), .notify = &.{} },
        .REPLCONF => .{ .reply = try handlers.handleReplconf(allocator, handler, command_parts), .notify = &.{} },
        .PSYNC => .{ .reply = try handlers.handlePsync(allocator, handler, client_connection, command_parts), .notify = &.{} },
        .CONFIG => .{ .reply = try handlers.handleConfig(allocator, handler, command_parts), .notify = &.{} },
        .KEYS => .{ .reply = try handlers.handleKeys(allocator, handler, command_parts), .notify = &.{} },
        else => unreachable,
    };
}

fn runCommandWithPropagation(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    client_connection: *ClientConnection,
    command: Command,
    command_parts: [64]?[]const u8,
) !ImmediateCommandResult {
    const result = try executeCommand(allocator, handler, client_connection, command, command_parts);
    const notify = try collectNotifications(allocator, handler, command, command_parts, result);
    return .{ .reply = result.reply, .notify = notify };
}

fn collectNotifications(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    command: Command,
    command_parts: [64]?[]const u8,
    result: CommandResult,
) !?[]WriteOp {
    var notifications = std.ArrayList(WriteOp).init(allocator);
    errdefer notifications.deinit();

    if (replication.isReplicationCommand(command) and !replyIsError(result.reply)) {
        const part_count = countCommandParts(command_parts);
        const prop = try replication.propagateCommand(allocator, handler.replicas.items, command_parts, part_count);
        defer allocator.free(prop.ops);

        handler.recordPropagation(prop.bytes_len);
        for (prop.ops) |op| {
            if (handler.connection_by_id.getPtr(op.connection_id)) |conn| {
                if (conn.is_loading_snapshot) {
                    try handler.bufferReplicaWrite(op.connection_id, op.bytes);
                    allocator.free(op.bytes);
                    continue;
                }
            }
            try notifications.append(op);
        }
    }

    // Process result.notify (e.g. LPOP from RPUSH)
    if (result.notify.len > 0) {
        defer allocator.free(result.notify);
        for (result.notify) |op| {
            if (handler.connection_by_id.getPtr(op.connection_id)) |conn| {
                if (conn.is_loading_snapshot) {
                    try handler.bufferReplicaWrite(op.connection_id, op.bytes);
                    allocator.free(op.bytes);
                    continue;
                }
            }
            try notifications.append(op);
        }
    }

    if (notifications.items.len == 0) {
        return null;
    }

    return try notifications.toOwnedSlice();
}

fn replyIsError(reply: Reply) bool {
    return switch (reply) {
        .Error => true,
        else => false,
    };
}

fn dispatchImmediateCommand(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    connection: *ClientConnection,
    command: Command,
    command_parts: [64]?[]const u8,
) !ImmediateCommandResult {
    if (connection.in_transaction and command != .MULTI and command != .EXEC and command != .DISCARD) {
        try queueTransactionCommand(handler, connection, command, command_parts);
        return .{ .reply = Reply{ .SimpleString = "QUEUED" }, .notify = null };
    }

    switch (command) {
        .MULTI => return handleMulti(handler, connection),
        .DISCARD => return handleDiscard(handler, connection),
        .EXEC => return try handleExec(allocator, handler, connection),
        else => return try runCommandWithPropagation(allocator, handler, connection, command, command_parts),
    }
}

fn queueTransactionCommand(
    handler: *AppHandler,
    connection: *ClientConnection,
    command: Command,
    command_parts: [64]?[]const u8,
) !void {
    const part_count = countCommandParts(command_parts);
    var stored_parts = try handler.app_allocator.alloc([]const u8, part_count);
    errdefer handler.app_allocator.free(stored_parts);

    var idx: usize = 0;
    while (idx < part_count) : (idx += 1) {
        const source = command_parts[idx].?;
        stored_parts[idx] = try handler.app_allocator.dupe(u8, source);
    }

    try connection.transaction_queue.append(.{
        .command = command,
        .parts = stored_parts,
    });
}

fn countCommandParts(command_parts: [64]?[]const u8) usize {
    var count: usize = 0;
    while (count < command_parts.len) : (count += 1) {
        if (command_parts[count] == null) break;
    }
    return count;
}

fn renderReply(allocator: std.mem.Allocator, reply: Reply) ![]const u8 {
    var response_buffer = std.ArrayList(u8).init(allocator);
    errdefer response_buffer.deinit();

    try format.writeReply(response_buffer.writer(), reply);
    const bytes = try response_buffer.toOwnedSlice();
    return bytes;
}

fn emptyImmediate() ProcessResult {
    return .{ .Immediate = .{ .bytes = &[_]u8{}, .notify = null } };
}

fn makeImmediate(bytes: []const u8, notify: ?[]WriteOp) ProcessResult {
    return .{ .Immediate = .{ .bytes = bytes, .notify = notify } };
}

fn appendNotify(accumulator: *std.ArrayList(WriteOp), notify: []const WriteOp) !void {
    if (notify.len != 0) try accumulator.appendSlice(notify);
}

fn handleMulti(handler: *AppHandler, connection: *ClientConnection) ImmediateCommandResult {
    if (connection.in_transaction) {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.NestedMulti } }, .notify = null };
    }

    handler.clearTransactionQueue(connection);
    connection.in_transaction = true;
    return .{ .reply = Reply{ .SimpleString = "OK" }, .notify = null };
}

fn handleDiscard(handler: *AppHandler, connection: *ClientConnection) ImmediateCommandResult {
    if (!connection.in_transaction) {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.DiscardWithoutMulti } }, .notify = null };
    }

    handler.clearTransactionQueue(connection);
    connection.in_transaction = false;
    return .{ .reply = Reply{ .SimpleString = "OK" }, .notify = null };
}

fn handleExec(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    connection: *ClientConnection,
) !ImmediateCommandResult {
    if (!connection.in_transaction) {
        return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ExecWithoutMulti } }, .notify = null };
    }

    connection.in_transaction = false;
    defer handler.clearTransactionQueue(connection);

    const result = try executeQueuedTransaction(allocator, handler, connection);
    const notify = try wrapInMultiExec(allocator, handler, result.notify);

    return .{ .reply = result.reply, .notify = notify };
}

fn executeQueuedTransaction(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    connection: *ClientConnection,
) !CommandResult {
    var replies = std.ArrayList(Reply).init(allocator);
    errdefer replies.deinit();

    var notifications = std.ArrayList(WriteOp).init(allocator);
    errdefer notifications.deinit();

    for (connection.transaction_queue.items) |queued| {
        var parts: [64]?[]const u8 = undefined;
        @memset(parts[0..], null);
        for (queued.parts, 0..) |p, i| {
            parts[i] = p;
        }

        const result = try runCommandWithPropagation(allocator, handler, connection, queued.command, parts);
        try replies.append(result.reply);
        if (result.notify) |ops| try notifications.appendSlice(ops);
    }

    return .{
        .reply = Reply{ .Array = try replies.toOwnedSlice() },
        .notify = try notifications.toOwnedSlice(),
    };
}

fn wrapInMultiExec(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    ops: []WriteOp,
) !?[]WriteOp {
    if (ops.len == 0) return null;
    defer allocator.free(ops);

    var final = std.ArrayList(WriteOp).init(allocator);
    errdefer final.deinit();

    const replicas = handler.replicas.items;
    var args: [64]?[]const u8 = undefined;
    @memset(args[0..], null);

    args[0] = "MULTI";
    const multi = try replication.propagateCommand(allocator, replicas, args, 1);
    handler.recordPropagation(multi.bytes_len);
    try final.appendSlice(multi.ops);

    try final.appendSlice(ops);

    args[0] = "EXEC";
    const exec = try replication.propagateCommand(allocator, replicas, args, 1);
    handler.recordPropagation(exec.bytes_len);
    try final.appendSlice(exec.ops);

    return try final.toOwnedSlice();
}
