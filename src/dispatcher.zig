const std = @import("std");
const Command = @import("command.zig").Command;
const handlers = @import("handlers.zig");
const AppHandler = @import("state.zig").AppHandler;
const ClientConnection = @import("state.zig").ClientConnection;
const format = @import("resp/format.zig");

const Reply = @import("reply.zig").Reply;
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
    const command = Command.fromSlice(command_str) orelse return emptyImmediate();

    return switch (command) {
        .BLPOP => handleBlockingBlpop(request_allocator, handler, client_connection, command_parts),
        .XREAD => handleBlockingXread(request_allocator, handler, client_connection, command_parts),
        else => blk: {
            const immediate = try processImmediateCommand(request_allocator, handler, command, command_parts);
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
        .XREAD => unreachable,
    };

    const notify_slice: ?[]WriteOp = if (notify_acc.items.len != 0)
        try notify_acc.toOwnedSlice()
    else
        null;

    return .{ .reply = reply, .notify = notify_slice };
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
