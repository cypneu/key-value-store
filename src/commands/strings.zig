const std = @import("std");

const Services = @import("../services.zig").Services;
const reply_mod = @import("reply.zig");
const Reply = reply_mod.Reply;
const ErrorKind = reply_mod.ErrorKind;
const wrongTypeReply = reply_mod.wrongTypeReply;

pub fn handleEcho(value: ?[]const u8) !Reply {
    return Reply{ .BulkString = value };
}

pub fn handleGet(services: *Services, key: []const u8) !Reply {
    if (services.list_store.contains(key)) return wrongTypeReply();
    if (services.stream_store.contains(key)) return wrongTypeReply();
    return Reply{ .BulkString = services.string_store.get(key) };
}

pub fn handleType(services: *Services, key: []const u8) !Reply {
    if (services.string_store.contains(key)) return Reply{ .SimpleString = "string" };
    if (services.list_store.contains(key)) return Reply{ .SimpleString = "list" };
    if (services.stream_store.contains(key)) return Reply{ .SimpleString = "stream" };

    return Reply{ .SimpleString = "none" };
}

pub fn handleSet(services: *Services, key: []const u8, value_slice: []const u8, px_ms: ?u64) !Reply {
    const expiration_us = if (px_ms) |ttl_ms| calculateExpiration(ttl_ms) else null;

    services.list_store.delete(key);
    services.stream_store.delete(key);

    try services.string_store.set(key, value_slice, expiration_us);
    return .{ .SimpleString = "OK" };
}

pub fn handleIncr(services: *Services, key: []const u8) !Reply {
    if (services.list_store.contains(key)) return wrongTypeReply();
    if (services.stream_store.contains(key)) return wrongTypeReply();

    const current = services.string_store.getWithExpiration(key) orelse {
        const initial: i64 = 1;
        var buf: [32]u8 = undefined;
        const slice = try std.fmt.bufPrint(&buf, "{d}", .{initial});
        try services.string_store.set(key, slice, null);
        return .{ .Integer = initial };
    };

    const parsed = std.fmt.parseInt(i64, current.data, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };
    const incremented = std.math.add(i64, parsed, 1) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };

    var buf: [32]u8 = undefined;
    const slice = try std.fmt.bufPrint(&buf, "{d}", .{incremented});
    try services.string_store.set(key, slice, current.expiration_us);

    return .{ .Integer = incremented };
}

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}
