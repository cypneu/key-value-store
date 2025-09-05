const std = @import("std");
const format = @import("resp/format.zig");
const db = @import("data_structures/mod.zig");
const ClientConnection = @import("state.zig").ClientConnection;
const AppHandler = @import("state.zig").AppHandler;
const Reply = @import("reply.zig").Reply;
const ErrorKind = @import("reply.zig").ErrorKind;
const Notify = @import("state.zig").Notify;

pub const CommandOutcome = struct {
    reply: Reply,
    notify: []Notify,
};

fn stringArrayReply(allocator: std.mem.Allocator, items: []const []const u8) !Reply {
    var arr = try allocator.alloc(Reply, items.len);
    for (items, 0..) |s, i| {
        arr[i] = Reply{ .BulkString = s };
    }
    return Reply{ .Array = arr };
}

fn stringArrayFromRangeView(allocator: std.mem.Allocator, rv: db.RangeView([]const u8)) !Reply {
    const total = rv.first.len + rv.second.len;
    var arr = try allocator.alloc(Reply, total);

    for (rv.first, 0..) |s, i| {
        arr[i] = Reply{ .BulkString = s };
    }
    for (rv.second, 0..) |s, i| {
        arr[i] = Reply{ .BulkString = s };
    }

    return Reply{ .Array = arr };
}

pub fn handleEcho(args: [64]?[]const u8) !Reply {
    const content = args[1] orelse return .{ .BulkString = null };
    return Reply{ .BulkString = content };
}

pub fn handleGet(string_store: *db.StringStore, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    return Reply{ .BulkString = string_store.get(key) };
}

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}

pub fn handleSet(store: *db.StringStore, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const value_slice = args[2] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const px_command = args[3] orelse "";

    var expiration_us: ?i64 = null;
    if (std.ascii.eqlIgnoreCase(px_command, "PX")) {
        const px_value_slice = args[4] orelse return .{ .Error = .{ .kind = ErrorKind.Syntax } };
        const px_value_ms = std.fmt.parseInt(u64, px_value_slice, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };
        expiration_us = calculateExpiration(px_value_ms);
    }

    try store.set(key, value_slice, expiration_us);
    return .{ .SimpleString = "OK" };
}

pub fn handleLpush(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !CommandOutcome {
    return handlePush(true, allocator, handler, args);
}

pub fn handleRpush(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !CommandOutcome {
    return handlePush(false, allocator, handler, args);
}

fn handlePush(comptime is_left: bool, allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !CommandOutcome {
    const key = args[1] orelse return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } }, .notify = &.{} };

    var length: u64 = 0;
    var pushed: usize = 0;
    for (args[2..]) |value_opt| {
        const value = value_opt orelse break;
        length = try handler.list_store.push(key, value, is_left);
        pushed += 1;
    }

    const notify = try handler.drainWaitersForKey(allocator, key, pushed);

    return .{
        .reply = Reply{ .Integer = @intCast(length) },
        .notify = notify,
    };
}

pub fn handleLrange(allocator: std.mem.Allocator, list_store: *db.ListStore, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const start_index_slice = args[2] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const end_index_slice = args[3] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };

    const start_index = std.fmt.parseInt(i64, start_index_slice, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };
    const end_index = std.fmt.parseInt(i64, end_index_slice, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };

    const range_view = list_store.lrange(key, start_index, end_index);
    return try stringArrayFromRangeView(allocator, range_view);
}

pub fn handleLlen(list_store: *db.ListStore, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = .ArgNum } };
    const length = list_store.length_by_key(key);
    return Reply{ .Integer = @intCast(length) };
}

pub fn handleLpop(allocator: std.mem.Allocator, list_store: *db.ListStore, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = .ArgNum } };
    const count_slice = args[2] orelse "1";
    const count = std.fmt.parseInt(u64, count_slice, 10) catch 1;

    const popped_values = try list_store.lpop(key, count, allocator);

    return switch (popped_values.len) {
        0 => Reply{ .BulkString = null },
        1 => Reply{ .BulkString = popped_values[0] },
        else => try stringArrayReply(allocator, popped_values),
    };
}

pub fn handleBlpop(allocator: std.mem.Allocator, handler: *AppHandler, client_connection: *ClientConnection, args: [64]?[]const u8) !union(enum) { Value: Reply, Blocked } {
    const key = args[1] orelse return .{ .Value = .{ .Error = .{ .kind = ErrorKind.ArgNum } } };
    const timeout_slice = args[2] orelse return .{ .Value = .{ .Error = .{ .kind = ErrorKind.ArgNum } } };

    const timeout_secs = std.fmt.parseFloat(f64, timeout_slice) catch return .{ .Value = .{ .Error = .{ .kind = ErrorKind.NotInteger } } };
    if (timeout_secs < 0) return .{ .Value = .{ .Error = .{ .kind = ErrorKind.NotInteger } } };

    const popped_values = try handler.list_store.lpop(key, 1, allocator);
    if (popped_values.len > 0) {
        var arr = try allocator.alloc(Reply, 2);
        arr[0] = Reply{ .BulkString = key };
        arr[1] = Reply{ .BulkString = popped_values[0] };
        return .{ .Value = .{ .Array = arr } };
    }

    var gop = try handler.blocked_clients_by_key.getOrPut(key);
    if (!gop.found_existing) {
        gop.key_ptr.* = try handler.app_allocator.dupe(u8, key);
        gop.value_ptr.* = .{};
    }

    const NodeType = std.DoublyLinkedList(std.posix.fd_t).Node;
    const node_ptr = try handler.app_allocator.create(NodeType);
    node_ptr.* = .{ .data = client_connection.fd };
    gop.value_ptr.append(node_ptr);

    client_connection.*.blocking_key = gop.key_ptr.*;
    client_connection.*.blocking_node = node_ptr;

    if (timeout_secs > 0) {
        const now_us: i64 = std.time.microTimestamp();
        const delta_us: i64 = @intFromFloat(timeout_secs * @as(f64, @floatFromInt(std.time.us_per_s)));
        const deadline_us: i64 = now_us + delta_us;
        client_connection.*.deadline_us = deadline_us;

        try handler.timeouts.add(.{ .deadline_us = deadline_us, .fd = client_connection.fd });
    } else {
        client_connection.*.deadline_us = null;
    }

    return .Blocked;
}
