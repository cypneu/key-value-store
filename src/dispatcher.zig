const std = @import("std");
const RESP = @import("resp/parser.zig").RESP;
const Command = @import("command.zig").Command;
const handlers = @import("handlers.zig");
const AppHandler = @import("state.zig").AppHandler;
const ClientConnection = @import("state.zig").ClientConnection;
const format = @import("resp/format.zig");

const Reply = @import("reply.zig").Reply;
const Notify = @import("state.zig").Notify;

pub const ProcessResult = union(enum) {
    Immediate: struct { bytes: []const u8, notify: ?[]Notify },
    Blocked,
};

pub fn processRequest(handler: *AppHandler, request_allocator: std.mem.Allocator, request_data: []const u8, client_connection: *ClientConnection) !ProcessResult {
    const commands = try RESP.parse(request_data);

    var response_buffer = std.ArrayList(u8).init(request_allocator);
    const writer = response_buffer.writer();
    var notify_acc = std.ArrayList(Notify).init(request_allocator);

    for (commands) |command_parts_optional| {
        const command_parts = command_parts_optional orelse continue;
        if (command_parts.len == 0) continue;

        const command_str = command_parts[0] orelse continue;
        const command = Command.fromSlice(command_str) orelse continue;

        if (command == .BLPOP) {
            const blpop_result = try handlers.handleBlpop(request_allocator, handler, client_connection, command_parts);
            return switch (blpop_result) {
                .Value => |reply| {
                    try format.writeReply(writer, reply);
                    return ProcessResult{ .Immediate = .{ .bytes = try response_buffer.toOwnedSlice(), .notify = null } };
                },
                .Blocked => ProcessResult.Blocked,
            };
        }

        const reply: Reply = switch (command) {
            .PING => Reply{ .SimpleString = "PONG" },
            .ECHO => try handlers.handleEcho(command_parts),
            .GET => try handlers.handleGet(handler.string_store, command_parts),
            .SET => try handlers.handleSet(handler.string_store, command_parts),

            .LPUSH => blk: {
                const out = try handlers.handleLpush(request_allocator, handler, command_parts);
                if (out.notify.len != 0) try notify_acc.appendSlice(out.notify);
                break :blk out.reply;
            },
            .RPUSH => blk: {
                const out = try handlers.handleRpush(request_allocator, handler, command_parts);
                if (out.notify.len != 0) try notify_acc.appendSlice(out.notify);
                break :blk out.reply;
            },

            .LRANGE => try handlers.handleLrange(request_allocator, handler.list_store, command_parts),
            .LLEN => try handlers.handleLlen(handler.list_store, command_parts),
            .LPOP => try handlers.handleLpop(request_allocator, handler.list_store, command_parts),
            .BLPOP => unreachable,
        };

        try format.writeReply(writer, reply);
    }

    const notify_slice: ?[]Notify = if (notify_acc.items.len != 0)
        try notify_acc.toOwnedSlice()
    else
        null;

    return ProcessResult{ .Immediate = .{
        .bytes = try response_buffer.toOwnedSlice(),
        .notify = notify_slice,
    } };
}
