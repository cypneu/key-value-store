const std = @import("std");
const db = @import("data_structures/mod.zig");

pub const NULL_BULK_STRING = "$-1\r\n";
pub const OK_MESSAGE = "+OK\r\n";

pub const ERR_ARG_NUM = "ERR wrong number of arguments";
pub const ERR_SYNTAX = "ERR syntax error";
pub const ERR_NOT_INTEGER = "ERR value is not an integer or out of range";

pub fn simpleString(allocator: std.mem.Allocator, string: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "+{s}\r\n", .{string});
}

pub fn bulkString(allocator: std.mem.Allocator, string: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ string.len, string });
}

pub fn integer(allocator: std.mem.Allocator, value: u64) ![]const u8 {
    return std.fmt.allocPrint(allocator, ":{}\r\n", .{value});
}

pub fn stringArray(allocator: std.mem.Allocator, strings: []const []const u8) ![]const u8 {
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    const writer = buffer.writer();
    try writer.print("*{d}\r\n", .{strings.len});
    for (strings) |s| {
        try writer.print("${d}\r\n{s}\r\n", .{ s.len, s });
    }

    return buffer.toOwnedSlice();
}

pub fn stringArrayRange(
    allocator: std.mem.Allocator,
    view: db.RangeView([]const u8),
) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const writer = buf.writer();
    const total = view.first.len + view.second.len;

    try writer.print("*{d}\r\n", .{total});
    for (view.first) |s| try writer.print("${d}\r\n{s}\r\n", .{ s.len, s });
    for (view.second) |s| try writer.print("${d}\r\n{s}\r\n", .{ s.len, s });

    return buf.toOwnedSlice();
}

pub fn simpleError(allocator: std.mem.Allocator, message: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "-{s}\r\n", .{message});
}
