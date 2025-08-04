const std = @import("std");
const format = @import("format.zig");
const db = @import("data_structures/mod.zig");

pub fn handleEcho(allocator: std.mem.Allocator, args: [64]?[]const u8) ![]const u8 {
    const content = args[1] orelse return format.NULL_BULK_STRING;
    return try format.bulkString(allocator, content);
}

pub fn handleGet(allocator: std.mem.Allocator, string_store: *db.StringStore, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);

    if (string_store.get(key)) |value_data| {
        return format.bulkString(allocator, value_data);
    } else {
        return format.NULL_BULK_STRING;
    }
}

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}

pub fn handleSet(allocator: std.mem.Allocator, store: *db.StringStore, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);
    const value_slice = args[2] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);
    const px_command = args[3] orelse "";

    var expiration_us: ?i64 = null;
    if (std.ascii.eqlIgnoreCase(px_command, "PX")) {
        const px_value_slice = args[4] orelse return format.simpleError(allocator, format.ERR_SYNTAX);
        const px_value_ms = std.fmt.parseInt(u64, px_value_slice, 10) catch return format.simpleError(allocator, format.ERR_NOT_INTEGER);
        expiration_us = calculateExpiration(px_value_ms);
    }

    try store.set(key, value_slice, expiration_us);
    return format.OK_MESSAGE;
}

pub fn handleLpush(alloc: std.mem.Allocator, ls: *db.ListStore, args: [64]?[]const u8) ![]const u8 {
    return handlePush(true, alloc, ls, args);
}

pub fn handleRpush(alloc: std.mem.Allocator, ls: *db.ListStore, args: [64]?[]const u8) ![]const u8 {
    return handlePush(false, alloc, ls, args);
}

fn handlePush(comptime is_left: bool, allocator: std.mem.Allocator, list_store: *db.ListStore, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);

    var length: u64 = 0;
    for (args[2..]) |value_opt| {
        const value = value_opt orelse break;
        length = try list_store.push(key, value, is_left);
    }

    return format.integer(allocator, length);
}

pub fn handleLrange(allocator: std.mem.Allocator, list_store: *db.ListStore, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);
    const start_index_slice = args[2] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);
    const end_index_slice = args[3] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);

    const start_index = std.fmt.parseInt(i64, start_index_slice, 10) catch return format.simpleError(allocator, format.ERR_NOT_INTEGER);
    const end_index = std.fmt.parseInt(i64, end_index_slice, 10) catch return format.simpleError(allocator, format.ERR_NOT_INTEGER);

    const range_view = list_store.lrange(key, start_index, end_index);
    return format.stringArrayRange(allocator, range_view);
}

pub fn handleLlen(allocator: std.mem.Allocator, list_store: *db.ListStore, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);
    const length = list_store.length_by_key(key);
    return format.integer(allocator, length);
}

pub fn handleLpop(allocator: std.mem.Allocator, list_store: *db.ListStore, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return format.simpleError(allocator, format.ERR_ARG_NUM);
    const count_slice = args[2] orelse "1";
    const count = std.fmt.parseInt(u64, count_slice, 10) catch 1;

    const popped_values = try list_store.lpop(key, count, allocator);

    return switch (popped_values.len) {
        0 => format.NULL_BULK_STRING,
        1 => format.bulkString(allocator, popped_values[0]),
        else => format.stringArray(allocator, popped_values),
    };
}
