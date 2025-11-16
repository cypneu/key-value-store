const std = @import("std");
const Command = @import("command.zig").Command;
const handlers = @import("handlers.zig");
const AppHandler = @import("state.zig").AppHandler;
const ClientConnection = @import("state.zig").ClientConnection;
const format = @import("resp/format.zig");

const Reply = @import("reply.zig").Reply;
const ErrorKind = @import("reply.zig").ErrorKind;
const WriteOp = @import("state.zig").WriteOp;

pub const ProcessResult = union(enum) {
    Immediate: struct { bytes: []const u8, notify: ?[]WriteOp },
    Blocked,
};

const ImmediateCommandResult = struct {
    reply: Reply,
    notify: ?[]WriteOp,
};

pub fn dispatchCommand(handler: *AppHandler, request_allocator: std.mem.Allocator, command_parts: [64]?[]const u8, client_connection: *ClientConnection) !ProcessResult {
    const command_str = command_parts[0] orelse return emptyImmediate();
    const command = Command.fromSlice(command_str) orelse {
        const bytes = try renderReply(request_allocator, Reply{ .Error = .{ .kind = ErrorKind.UnknownCommand } });
        return makeImmediate(bytes, null);
    };

    return switch (command) {
        .BLPOP => handleBlockingBlpop(request_allocator, handler, client_connection, command_parts),
        .XREAD => handleBlockingXread(request_allocator, handler, client_connection, command_parts),
        else => blk: {
            const immediate = try processImmediateCommand(request_allocator, handler, client_connection, command, command_parts);
            const bytes = try renderReply(request_allocator, immediate.reply);
            break :blk makeImmediate(bytes, immediate.notify);
        },
    };
}

fn handleBlockingBlpop(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    connection: *ClientConnection,
    command_parts: [64]?[]const u8,
) !ProcessResult {
    const blpop_result = try handlers.handleBlpop(allocator, handler, connection, command_parts);
    return switch (blpop_result) {
        .Value => |reply| makeImmediate(try renderReply(allocator, reply), null),
        .Blocked => ProcessResult.Blocked,
    };
}

fn handleBlockingXread(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    connection: *ClientConnection,
    command_parts: [64]?[]const u8,
) !ProcessResult {
    const xread_result = try handlers.handleXread(allocator, handler, connection, command_parts);
    return switch (xread_result) {
        .Value => |reply| makeImmediate(try renderReply(allocator, reply), null),
        .Blocked => ProcessResult.Blocked,
    };
}

fn processImmediateCommand(
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

    var notify_acc = std.ArrayList(WriteOp).init(allocator);
    errdefer notify_acc.deinit();

    const immediate: ImmediateCommandResult = switch (command) {
        .EXEC => try handleExec(allocator, handler, connection),
        .PING => .{ .reply = Reply{ .SimpleString = "PONG" }, .notify = null },
        .ECHO => .{ .reply = try handlers.handleEcho(command_parts), .notify = null },
        .GET => .{ .reply = try handlers.handleGet(handler, command_parts), .notify = null },
        .TYPE => .{ .reply = try handlers.handleType(handler, command_parts), .notify = null },
        .SET => .{ .reply = try handlers.handleSet(handler, command_parts), .notify = null },
        .INCR => .{ .reply = try handlers.handleIncr(handler, command_parts), .notify = null },

        .LPUSH => blk: {
            const out = try handlers.handleLpush(allocator, handler, command_parts);
            try appendNotify(&notify_acc, out.notify);
            break :blk .{ .reply = out.reply, .notify = null };
        },
        .RPUSH => blk: {
            const out = try handlers.handleRpush(allocator, handler, command_parts);
            try appendNotify(&notify_acc, out.notify);
            break :blk .{ .reply = out.reply, .notify = null };
        },
        .LRANGE => .{ .reply = try handlers.handleLrange(allocator, handler, command_parts), .notify = null },
        .LLEN => .{ .reply = try handlers.handleLlen(handler, command_parts), .notify = null },
        .LPOP => .{ .reply = try handlers.handleLpop(allocator, handler, command_parts), .notify = null },
        .BLPOP => unreachable,
        .XADD => blk: {
            const out = try handlers.handleXadd(allocator, handler, command_parts);
            try appendNotify(&notify_acc, out.notify);
            break :blk .{ .reply = out.reply, .notify = null };
        },
        .XRANGE => .{ .reply = try handlers.handleXrange(allocator, handler, command_parts), .notify = null },
        .XREAD => unreachable,
        .MULTI => handleMulti(handler, connection),
        .DISCARD => handleDiscard(handler, connection),
    };

    const notify_slice: ?[]WriteOp = if (immediate.notify != null) immediate.notify else blk: {
        if (notify_acc.items.len != 0) {
            break :blk try notify_acc.toOwnedSlice();
        } else {
            break :blk null;
        }
    };

    return .{ .reply = immediate.reply, .notify = notify_slice };
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
    errdefer handler.clearTransactionQueue(connection);

    var replies = std.ArrayList(Reply).init(allocator);
    errdefer replies.deinit();

    var notify_acc = std.ArrayList(WriteOp).init(allocator);
    errdefer notify_acc.deinit();

    for (connection.transaction_queue.items) |queued| {
        var queued_parts: [64]?[]const u8 = undefined;
        @memset(queued_parts[0..], null);
        for (queued.parts, 0..) |part, idx| {
            queued_parts[idx] = part;
        }

        const immediate = try executeCommand(allocator, handler, queued.command, queued_parts);
        try replies.append(immediate.reply);

        if (immediate.notify) |ns| try notify_acc.appendSlice(ns);
    }

    const replies_slice = try replies.toOwnedSlice();
    handler.clearTransactionQueue(connection);

    const notify_slice: ?[]WriteOp = if (notify_acc.items.len != 0)
        try notify_acc.toOwnedSlice()
    else
        null;

    return .{ .reply = Reply{ .Array = replies_slice }, .notify = notify_slice };
}

fn executeCommand(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    command: Command,
    command_parts: [64]?[]const u8,
) !ImmediateCommandResult {
    var notify_acc = std.ArrayList(WriteOp).init(allocator);
    errdefer notify_acc.deinit();

    const reply: Reply = switch (command) {
        .PING => Reply{ .SimpleString = "PONG" },
        .ECHO => try handlers.handleEcho(command_parts),
        .GET => try handlers.handleGet(handler, command_parts),
        .TYPE => try handlers.handleType(handler, command_parts),
        .SET => try handlers.handleSet(handler, command_parts),
        .INCR => try handlers.handleIncr(handler, command_parts),
        .LPUSH => blk: {
            const out = try handlers.handleLpush(allocator, handler, command_parts);
            try appendNotify(&notify_acc, out.notify);
            break :blk out.reply;
        },
        .RPUSH => blk: {
            const out = try handlers.handleRpush(allocator, handler, command_parts);
            try appendNotify(&notify_acc, out.notify);
            break :blk out.reply;
        },
        .LRANGE => try handlers.handleLrange(allocator, handler, command_parts),
        .LLEN => try handlers.handleLlen(handler, command_parts),
        .LPOP => try handlers.handleLpop(allocator, handler, command_parts),
        .BLPOP => unreachable,
        .XADD => blk: {
            const out = try handlers.handleXadd(allocator, handler, command_parts);
            try appendNotify(&notify_acc, out.notify);
            break :blk out.reply;
        },
        .XRANGE => try handlers.handleXrange(allocator, handler, command_parts),
        else => unreachable,
    };

    const notify_slice: ?[]WriteOp = if (notify_acc.items.len != 0)
        try notify_acc.toOwnedSlice()
    else
        null;

    return .{ .reply = reply, .notify = notify_slice };
}
