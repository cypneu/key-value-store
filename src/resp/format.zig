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
        .Array => |items| {
            try writer.print("*{d}\r\n", .{items.len});
            for (items) |it| try writeReply(writer, it);
        },
        .Error => |e| {
            const msg = switch (e.kind) {
                .ArgNum => "ERR wrong number of arguments",
                .Syntax => "ERR syntax error",
                .NotInteger => "ERR value is not an integer or out of range",
                .Custom => e.msg orelse "ERR",
            };
            try writer.print("-{s}\r\n", .{msg});
        },
    }
}
