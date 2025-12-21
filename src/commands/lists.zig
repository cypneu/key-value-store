const std = @import("std");

const blocking = @import("../features/blocking.zig");
const replication = @import("../features/replication.zig");
const connection = @import("../network/connection.zig");
const Connection = connection.Connection;
const services_mod = @import("../services.zig");
const Services = services_mod.Services;
const WriteOp = services_mod.WriteOp;
const list_store = @import("../storage/list.zig");
const RangeView = list_store.RangeView;
const dispatcher = @import("dispatcher.zig");
const CommandOutcome = dispatcher.CommandOutcome;
const Output = dispatcher.Output;
const reply_mod = @import("reply.zig");
const Reply = reply_mod.Reply;
const wrongTypeReply = reply_mod.wrongTypeReply;
const stringArrayReply = reply_mod.stringArrayReply;

pub fn handleLpush(
    allocator: std.mem.Allocator,
    services: *Services,
    key: []const u8,
    values: []const []const u8,
) !Output {
    return handlePush(true, allocator, services, key, values);
}

pub fn handleRpush(
    allocator: std.mem.Allocator,
    services: *Services,
    key: []const u8,
    values: []const []const u8,
) !Output {
    return handlePush(false, allocator, services, key, values);
}

fn handlePush(
    comptime is_left: bool,
    allocator: std.mem.Allocator,
    services: *Services,
    key: []const u8,
    values: []const []const u8,
) !Output {
    if (services.string_store.contains(key) or services.stream_store.contains(key)) {
        return .{ .reply = wrongTypeReply() };
    }

    var length: u64 = 0;
    var pushed: usize = 0;
    for (values) |value| {
        length = try services.list_store.push(key, value, is_left);
        pushed += 1;
    }

    const drain_result = try services.drainWaitersForKey(allocator, key, pushed);

    var side_effects = std.ArrayList(WriteOp).init(allocator);
    errdefer side_effects.deinit();

    try side_effects.appendSlice(drain_result.ops);

    if (drain_result.popped > 0) {
        var lpop_args: [3][]const u8 = .{ "LPOP", key, "" };

        var count_buf: [32]u8 = undefined;

        if (drain_result.popped > 1) {
            const count_str = try std.fmt.bufPrint(&count_buf, "{d}", .{drain_result.popped});
            lpop_args[2] = count_str;
        }

        const arg_count: usize = if (drain_result.popped > 1) 3 else 2;
        const prop = try replication.propagateCommand(allocator, services.replication.replicas.items, lpop_args[0..arg_count]);
        services.replication.recordOffset(prop.bytes_len);
        try side_effects.appendSlice(prop.ops);
    }

    return .{
        .reply = Reply{ .Integer = @intCast(length) },
        .side_effects = try side_effects.toOwnedSlice(),
    };
}

pub fn handleLrange(allocator: std.mem.Allocator, services: *Services, key: []const u8, start_index: i64, end_index: i64) !Reply {
    if (services.string_store.contains(key)) return wrongTypeReply();
    if (services.stream_store.contains(key)) return wrongTypeReply();

    const range_view = services.list_store.lrange(key, start_index, end_index);
    return try stringArrayFromRangeView(allocator, range_view);
}

pub fn handleLlen(services: *Services, key: []const u8) !Reply {
    if (services.string_store.contains(key)) return wrongTypeReply();
    if (services.stream_store.contains(key)) return wrongTypeReply();
    const length = services.list_store.length_by_key(key);
    return Reply{ .Integer = @intCast(length) };
}

pub fn handleLpop(allocator: std.mem.Allocator, services: *Services, key: []const u8, count: u64) !Reply {
    if (services.string_store.contains(key)) return wrongTypeReply();
    if (services.stream_store.contains(key)) return wrongTypeReply();

    const popped_values = try services.list_store.lpop(key, count, allocator);

    return switch (popped_values.len) {
        0 => Reply{ .BulkString = null },
        1 => Reply{ .BulkString = popped_values[0] },
        else => try stringArrayReply(allocator, popped_values),
    };
}

pub fn handleBlpop(
    allocator: std.mem.Allocator,
    services: *Services,
    client_connection: *Connection,
    key: []const u8,
    timeout_secs: f64,
) !CommandOutcome {
    ensureListOperationAllowed(services, key) catch |err| {
        const r: Reply = switch (err) {
            error.WrongType => wrongTypeReply(),
            else => return err,
        };
        return .{ .Immediate = .{ .reply = r } };
    };

    if (try tryImmediateBlpop(allocator, services, key)) |outcome| {
        return .{ .Immediate = outcome };
    }

    try services.blocking.registerListBlockingClient(client_connection.id, key, timeout_secs);
    return .{ .Blocked = .{} };
}

pub fn handleBlpopNonBlocking(allocator: std.mem.Allocator, services: *Services, key: []const u8) !Output {
    ensureListOperationAllowed(services, key) catch |err| {
        const r: Reply = switch (err) {
            error.WrongType => wrongTypeReply(),
            else => return err,
        };
        return .{ .reply = r };
    };

    if (try tryImmediateBlpop(allocator, services, key)) |outcome| {
        return outcome;
    }

    return .{ .reply = Reply{ .Array = null } };
}

fn stringArrayFromRangeView(allocator: std.mem.Allocator, rv: RangeView([]const u8)) !Reply {
    const total = rv.first.len + rv.second.len;
    var arr = try allocator.alloc(Reply, total);

    for (rv.first, 0..) |s, i| {
        arr[i] = Reply{ .BulkString = s };
    }
    for (rv.second, 0..) |s, i| {
        arr[rv.first.len + i] = Reply{ .BulkString = s };
    }

    return Reply{ .Array = arr };
}

fn ensureListOperationAllowed(services: *Services, key: []const u8) !void {
    if (services.string_store.contains(key) or services.stream_store.contains(key)) {
        return error.WrongType;
    }
}

fn tryImmediateBlpop(allocator: std.mem.Allocator, services: *Services, key: []const u8) !?Output {
    const popped_values = try services.list_store.lpop(key, 1, allocator);
    if (popped_values.len == 0) return null;

    var arr = try allocator.alloc(Reply, 2);
    const key_copy = try allocator.dupe(u8, key);
    arr[0] = Reply{ .BulkString = key_copy };
    arr[1] = Reply{ .BulkString = popped_values[0] };

    const reply = Reply{ .Array = arr };

    const args = [_][]const u8{ "LPOP", key };
    const prop = try replication.propagateCommand(allocator, services.replication.replicas.items, args[0..]);
    services.replication.recordOffset(prop.bytes_len);

    return Output{ .reply = reply, .side_effects = prop.ops };
}
