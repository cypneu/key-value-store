const std = @import("std");
const format = @import("resp/format.zig");
const db = @import("data_structures/mod.zig");
const ClientConnection = @import("state.zig").ClientConnection;
const AppHandler = @import("state.zig").AppHandler;
const Reply = @import("reply.zig").Reply;
const ErrorKind = @import("reply.zig").ErrorKind;
const WriteOp = @import("state.zig").WriteOp;

pub const CommandOutcome = struct {
    reply: Reply,
    notify: []WriteOp,
};

fn wrongTypeReply() Reply {
    return Reply{ .Error = .{ .kind = ErrorKind.WrongType } };
}

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

fn streamEntryFieldsReply(allocator: std.mem.Allocator, fields: []const db.StreamEntryField) !Reply {
    const total = fields.len * 2;
    var arr = try allocator.alloc(Reply, total);

    for (fields, 0..) |field, i| {
        const base = i * 2;
        arr[base] = Reply{ .BulkString = field.name };
        arr[base + 1] = Reply{ .BulkString = field.value };
    }

    return Reply{ .Array = arr };
}

fn streamEntriesReply(allocator: std.mem.Allocator, entries: []*const db.StreamEntry) ![]Reply {
    var outer = try allocator.alloc(Reply, entries.len);
    for (entries, 0..) |entry_ptr, idx| {
        const entry = entry_ptr.*;
        var entry_reply = try allocator.alloc(Reply, 2);
        var id_buf: [64]u8 = undefined;
        const id_slice = try entry.id.format(id_buf[0..]);
        const id_copy = try allocator.dupe(u8, id_slice);
        entry_reply[0] = Reply{ .BulkString = id_copy };
        entry_reply[1] = try streamEntryFieldsReply(allocator, entry.fields);
        outer[idx] = Reply{ .Array = entry_reply };
    }
    return outer;
}

fn streamReadResultReply(allocator: std.mem.Allocator, key: []const u8, entries: []*const db.StreamEntry) !Reply {
    const entry_reply = try streamEntriesReply(allocator, entries);
    var stream_pair = try allocator.alloc(Reply, 2);
    stream_pair[0] = Reply{ .BulkString = try allocator.dupe(u8, key) };
    stream_pair[1] = Reply{ .Array = entry_reply };
    return Reply{ .Array = stream_pair };
}

const StreamReadRequest = struct {
    key: []const u8,
    id: []const u8,
};

const BlpopRequest = struct {
    key: []const u8,
    timeout_secs: f64,
};

fn parseXreadRequests(allocator: std.mem.Allocator, args: [64]?[]const u8) ![]StreamReadRequest {
    const total_args = argumentCount(args);
    if (total_args < 4) return error.ArgNum;

    const streams_token = args[1] orelse return error.ArgNum;
    if (!std.ascii.eqlIgnoreCase(streams_token, "STREAMS")) {
        return error.Syntax;
    }

    const remaining = total_args - 2;
    if (remaining == 0 or (remaining % 2 != 0)) {
        return error.ArgNum;
    }

    const stream_count = remaining / 2;
    const keys_start: usize = 2;
    const ids_start = keys_start + stream_count;

    var requests = try allocator.alloc(StreamReadRequest, stream_count);
    errdefer allocator.free(requests);

    var idx: usize = 0;
    while (idx < stream_count) : (idx += 1) {
        requests[idx] = .{
            .key = args[keys_start + idx] orelse return error.ArgNum,
            .id = args[ids_start + idx] orelse return error.ArgNum,
        };
    }

    return requests;
}

fn readStreamsIntoReplies(allocator: std.mem.Allocator, handler: *AppHandler, requests: []const StreamReadRequest) !?[]Reply {
    var streams_reply = std.ArrayList(Reply).init(allocator);
    defer streams_reply.deinit();

    for (requests) |req| {
        if (handler.string_store.contains(req.key) or handler.list_store.contains(req.key)) {
            return error.WrongType;
        }

        const entries = handler.stream_store.xread(allocator, req.key, req.id) catch |err| {
            return switch (err) {
                error.InvalidRangeId => error.Syntax,
                else => err,
            };
        };

        if (entries.len == 0) continue;

        const stream_reply = try streamReadResultReply(allocator, req.key, entries);
        try streams_reply.append(stream_reply);
    }

    if (streams_reply.items.len == 0) {
        return null;
    }

    return try streams_reply.toOwnedSlice();
}

fn parseBlpopRequest(args: [64]?[]const u8) !BlpopRequest {
    const key = args[1] orelse return error.ArgNum;
    const timeout_slice = args[2] orelse return error.ArgNum;

    const timeout_secs = std.fmt.parseFloat(f64, timeout_slice) catch return error.NotInteger;
    if (timeout_secs < 0) return error.NotInteger;

    return BlpopRequest{ .key = key, .timeout_secs = timeout_secs };
}

fn ensureListOperationAllowed(handler: *AppHandler, key: []const u8) !void {
    if (handler.string_store.contains(key) or handler.stream_store.contains(key)) {
        return error.WrongType;
    }
}

fn tryImmediateBlpop(allocator: std.mem.Allocator, handler: *AppHandler, key: []const u8) !?Reply {
    const popped_values = try handler.list_store.lpop(key, 1, allocator);
    if (popped_values.len == 0) return null;

    var arr = try allocator.alloc(Reply, 2);
    arr[0] = Reply{ .BulkString = key };
    arr[1] = Reply{ .BulkString = popped_values[0] };
    return Reply{ .Array = arr };
}

fn registerBlockingClient(handler: *AppHandler, client_connection: *ClientConnection, key: []const u8, timeout_secs: f64) !void {
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
}

fn argumentCount(args: [64]?[]const u8) usize {
    var count: usize = 0;
    while (count < args.len) : (count += 1) {
        if (args[count] == null) break;
    }
    return count;
}

pub fn handleEcho(args: [64]?[]const u8) !Reply {
    const content = args[1] orelse return .{ .BulkString = null };
    return Reply{ .BulkString = content };
}

pub fn handleGet(handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    if (handler.list_store.contains(key)) return wrongTypeReply();
    if (handler.stream_store.contains(key)) return wrongTypeReply();
    return Reply{ .BulkString = handler.string_store.get(key) };
}

pub fn handleType(handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };

    if (handler.string_store.contains(key)) return Reply{ .SimpleString = "string" };
    if (handler.list_store.contains(key)) return Reply{ .SimpleString = "list" };
    if (handler.stream_store.contains(key)) return Reply{ .SimpleString = "stream" };

    return Reply{ .SimpleString = "none" };
}

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}

pub fn handleSet(handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const value_slice = args[2] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const px_command = args[3] orelse "";

    var expiration_us: ?i64 = null;
    if (std.ascii.eqlIgnoreCase(px_command, "PX")) {
        const px_value_slice = args[4] orelse return .{ .Error = .{ .kind = ErrorKind.Syntax } };
        const px_value_ms = std.fmt.parseInt(u64, px_value_slice, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };
        expiration_us = calculateExpiration(px_value_ms);
    }

    handler.list_store.delete(key);
    handler.stream_store.delete(key);

    try handler.string_store.set(key, value_slice, expiration_us);
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

    if (handler.string_store.contains(key) or handler.stream_store.contains(key)) {
        return .{ .reply = wrongTypeReply(), .notify = &.{} };
    }

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

pub fn handleLrange(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const start_index_slice = args[2] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const end_index_slice = args[3] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };

    const start_index = std.fmt.parseInt(i64, start_index_slice, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };
    const end_index = std.fmt.parseInt(i64, end_index_slice, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };

    if (handler.string_store.contains(key)) return wrongTypeReply();
    if (handler.stream_store.contains(key)) return wrongTypeReply();

    const range_view = handler.list_store.lrange(key, start_index, end_index);
    return try stringArrayFromRangeView(allocator, range_view);
}

pub fn handleLlen(handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = .ArgNum } };
    if (handler.string_store.contains(key)) return wrongTypeReply();
    if (handler.stream_store.contains(key)) return wrongTypeReply();
    const length = handler.list_store.length_by_key(key);
    return Reply{ .Integer = @intCast(length) };
}

pub fn handleLpop(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = .ArgNum } };
    const count_slice = args[2] orelse "1";
    const count = std.fmt.parseInt(u64, count_slice, 10) catch 1;

    if (handler.string_store.contains(key)) return wrongTypeReply();
    if (handler.stream_store.contains(key)) return wrongTypeReply();

    const popped_values = try handler.list_store.lpop(key, count, allocator);

    return switch (popped_values.len) {
        0 => Reply{ .BulkString = null },
        1 => Reply{ .BulkString = popped_values[0] },
        else => try stringArrayReply(allocator, popped_values),
    };
}

pub fn handleXadd(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const entry_id = args[2] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };

    if (handler.string_store.contains(key)) return wrongTypeReply();
    if (handler.list_store.contains(key)) return wrongTypeReply();

    const arg_count = argumentCount(args);
    if (arg_count < 5) return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const field_value_count = arg_count - 3;
    if ((field_value_count % 2) != 0) return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };

    var pairs = std.ArrayList(db.StreamFieldValue).init(allocator);
    defer pairs.deinit();

    var index: usize = 3;
    while (index < arg_count) : (index += 2) {
        const field = args[index].?;
        const value = args[index + 1].?;
        try pairs.append(.{ .field = field, .value = value });
    }

    if (pairs.items.len == 0) return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };

    const stored_entry_id = handler.stream_store.addEntry(allocator, key, entry_id, pairs.items) catch |err| {
        return switch (err) {
            error.EntryIdTooSmall => Reply{ .Error = .{ .kind = ErrorKind.XaddIdTooSmall } },
            error.EntryIdZero => Reply{ .Error = .{ .kind = ErrorKind.XaddIdNotGreaterThanZero } },
            error.InvalidEntryId => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
    };

    return Reply{ .BulkString = stored_entry_id };
}

pub fn handleXrange(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const start_id = args[2] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };
    const end_id = args[3] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };

    if (handler.string_store.contains(key)) return wrongTypeReply();
    if (handler.list_store.contains(key)) return wrongTypeReply();

    const entries = handler.stream_store.xrange(allocator, key, start_id, end_id) catch |err| {
        return switch (err) {
            error.InvalidRangeId => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
    };

    const outer = try streamEntriesReply(allocator, entries);
    return Reply{ .Array = outer };
}

pub fn handleXread(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const requests = parseXreadRequests(allocator, args) catch |err| {
        return switch (err) {
            error.ArgNum => Reply{ .Error = .{ .kind = ErrorKind.ArgNum } },
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
    };
    defer allocator.free(requests);

    const stream_replies = readStreamsIntoReplies(allocator, handler, requests) catch |err| {
        return switch (err) {
            error.WrongType => wrongTypeReply(),
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
    };
    return Reply{ .Array = stream_replies };
}

pub fn handleBlpop(allocator: std.mem.Allocator, handler: *AppHandler, client_connection: *ClientConnection, args: [64]?[]const u8) !union(enum) { Value: Reply, Blocked } {
    const request = parseBlpopRequest(args) catch |err| {
        return .{ .Value = switch (err) {
            error.ArgNum => Reply{ .Error = .{ .kind = ErrorKind.ArgNum } },
            error.NotInteger => Reply{ .Error = .{ .kind = ErrorKind.NotInteger } },
            else => return err,
        } };
    };

    ensureListOperationAllowed(handler, request.key) catch |err| {
        return .{ .Value = switch (err) {
            error.WrongType => wrongTypeReply(),
            else => return err,
        } };
    };

    if (try tryImmediateBlpop(allocator, handler, request.key)) |reply| {
        return .{ .Value = reply };
    }

    try registerBlockingClient(handler, client_connection, request.key, request.timeout_secs);
    return .Blocked;
}
