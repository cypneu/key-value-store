const std = @import("std");
const reply_mod = @import("../../commands/reply.zig");

pub const ErrorKind = reply_mod.ErrorKind;
pub const Reply = reply_mod.Reply;

pub fn writeReply(writer: anytype, r: Reply) !void {
    switch (r) {
        .SimpleString => |s| try writer.print("+{s}\r\n", .{s}),
        .BulkString => |opt| {
            if (opt) |s| try writer.print("${d}\r\n{s}\r\n", .{ s.len, s }) else try writer.writeAll("$-1\r\n");
        },
        .Integer => |n| try writer.print(":{d}\r\n", .{n}),
        .Array => |maybe_items| {
            if (maybe_items) |items| {
                try writer.print("*{d}\r\n", .{items.len});
                for (items) |it| try writeReply(writer, it);
            } else {
                try writer.writeAll("*-1\r\n");
            }
        },
        .Error => |e| {
            const msg = switch (e.kind) {
                .ArgNum => "ERR wrong number of arguments",
                .Syntax => "ERR syntax error",
                .NotInteger => "ERR value is not an integer or out of range",
                .WrongType => "WRONGTYPE Operation against a key holding the wrong kind of value",
                .XaddIdTooSmall => "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                .XaddIdNotGreaterThanZero => "ERR The ID specified in XADD must be greater than 0-0",
                .ExecWithoutMulti => "ERR EXEC without MULTI",
                .DiscardWithoutMulti => "ERR DISCARD without MULTI",
                .NestedMulti => "ERR MULTI calls can not be nested",
                .ExecAbort => "EXECABORT Transaction discarded because of previous errors.",
                .UnknownCommand => "ERR unknown command",
                .Loading => "LOADING Redis is loading the dataset in memory",
            };
            try writer.print("-{s}\r\n", .{msg});
        },
        .Bytes => |b| try writer.writeAll(b),
    }
}

pub fn renderReply(allocator: std.mem.Allocator, reply: Reply) ![]const u8 {
    var response_buffer = std.ArrayList(u8).init(allocator);
    errdefer response_buffer.deinit();
    try writeReply(response_buffer.writer(), reply);
    return try response_buffer.toOwnedSlice();
}
