const std = @import("std");
const db = @import("../data_structures/mod.zig");

const Reply = @import("../reply.zig").Reply;
const ErrorKind = @import("../reply.zig").ErrorKind;

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
                .UnknownCommand => "ERR unknown command",
            };
            try writer.print("-{s}\r\n", .{msg});
        },
    }
}
