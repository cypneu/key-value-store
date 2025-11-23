const std = @import("std");
const format = @import("resp/format.zig");
const db = @import("data_structures/mod.zig");
const ClientConnection = @import("state.zig").ClientConnection;
const AppHandler = @import("state.zig").AppHandler;
const Reply = @import("reply.zig").Reply;
const ErrorKind = @import("reply.zig").ErrorKind;
const WriteOp = @import("state.zig").WriteOp;
const replication = @import("replication.zig");

const DEFAULT_REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const INFO_MASTER = "role:master\r\nmaster_replid:" ++ DEFAULT_REPL_ID ++ "\r\nmaster_repl_offset:0\r\n";
const INFO_REPLICA = "role:slave\r\nmaster_replid:" ++ DEFAULT_REPL_ID ++ "\r\nmaster_repl_offset:0\r\n";

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

const BlpopRequest = struct {
    key: []const u8,
    timeout_secs: f64,
};

pub const StreamReadRequest = struct {
    key: []const u8,
    id: []const u8,
};

fn isLatestStreamId(id: []const u8) bool {
    return id.len == 1 and id[0] == '$';
}

fn latestStreamIdSnapshot(allocator: std.mem.Allocator, handler: *AppHandler, key: []const u8) ![]u8 {
    var id_buffer: [64]u8 = undefined;
    if (try handler.stream_store.formatLastEntryId(key, id_buffer[0..])) |formatted| {
        return try allocator.dupe(u8, formatted);
    }
    return try allocator.dupe(u8, "0-0");
}

fn resolveLatestStreamIds(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    requests: []StreamReadRequest,
    replacements: *std.ArrayList([]u8),
) !void {
    for (requests, 0..) |request, idx| {
        if (!isLatestStreamId(request.id)) continue;
        const snapshot = try latestStreamIdSnapshot(allocator, handler, request.key);
        try replacements.append(snapshot);
        requests[idx].id = snapshot;
    }
}

fn parseXreadRequests(allocator: std.mem.Allocator, args: [64]?[]const u8, streams_index: usize) ![]StreamReadRequest {
    const total_args = argumentCount(args);
    if (streams_index >= total_args) return error.ArgNum;

    const streams_token = args[streams_index] orelse return error.ArgNum;
    if (!std.ascii.eqlIgnoreCase(streams_token, "STREAMS")) {
        return error.Syntax;
    }

    const remaining = total_args - (streams_index + 1);
    if (remaining == 0 or (remaining % 2 != 0)) {
        return error.ArgNum;
    }

    const stream_count = remaining / 2;
    const keys_start: usize = streams_index + 1;
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

const ParsedXread = struct {
    block_ms: ?u64,
    requests: []StreamReadRequest,
};

fn parseXreadArguments(allocator: std.mem.Allocator, args: [64]?[]const u8) !ParsedXread {
    const total_args = argumentCount(args);
    if (total_args < 4) return error.ArgNum;

    var idx: usize = 1;
    var block_ms: ?u64 = null;

    if (idx < total_args) {
        const token = args[idx] orelse return error.ArgNum;
        if (std.ascii.eqlIgnoreCase(token, "BLOCK")) {
            const timeout_slice = args[idx + 1] orelse return error.ArgNum;
            block_ms = std.fmt.parseInt(u64, timeout_slice, 10) catch return error.NotInteger;
            idx += 2;
        }
    }

    const requests = try parseXreadRequests(allocator, args, idx);
    return ParsedXread{
        .block_ms = block_ms,
        .requests = requests,
    };
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

fn replyToBytes(allocator: std.mem.Allocator, reply: Reply) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    try format.writeReply(buf.writer(), reply);
    return try buf.toOwnedSlice();
}

fn tryResumeXreadConnection(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    connection: *ClientConnection,
) !?[]u8 {
    const block_state = connection.blocking_stream_state orelse return null;

    const stream_replies = readStreamsIntoReplies(allocator, handler, block_state.requests) catch |err| {
        handler.clearStreamBlockingState(connection);
        const reply = switch (err) {
            error.WrongType => wrongTypeReply(),
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
        return try replyToBytes(allocator, reply);
    };

    if (stream_replies == null) return null;

    const bytes = try replyToBytes(allocator, Reply{ .Array = stream_replies });
    handler.clearStreamBlockingState(connection);
    return bytes;
}

fn notifyStreamWaiters(allocator: std.mem.Allocator, handler: *AppHandler, key: []const u8) ![]WriteOp {
    const wait_list_ptr = handler.blocked_stream_clients_by_key.getPtr(key) orelse return &.{};

    var notifications = std.ArrayList(WriteOp).init(allocator);
    errdefer notifications.deinit();

    var node_ptr_opt = wait_list_ptr.first;
    while (node_ptr_opt) |node_ptr| {
        const next = node_ptr.next;

        const id = node_ptr.data;
        if (handler.connection_by_id.getPtr(id)) |connection| {
            if (try tryResumeXreadConnection(allocator, handler, connection)) |bytes| {
                try notifications.append(.{ .connection_id = id, .bytes = bytes });
            }
        } else {
            wait_list_ptr.remove(node_ptr);
            handler.app_allocator.destroy(node_ptr);
        }

        node_ptr_opt = next;
    }

    if (notifications.items.len == 0) {
        return &.{};
    }
    return try notifications.toOwnedSlice();
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

fn tryImmediateBlpop(allocator: std.mem.Allocator, handler: *AppHandler, key: []const u8) !?CommandOutcome {
    const popped_values = try handler.list_store.lpop(key, 1, allocator);
    if (popped_values.len == 0) return null;

    var arr = try allocator.alloc(Reply, 2);
    arr[0] = Reply{ .BulkString = key };
    arr[1] = Reply{ .BulkString = popped_values[0] };

    const reply = Reply{ .Array = arr };

    var args: [64]?[]const u8 = undefined;
    args[0] = "LPOP";
    args[1] = key;

    const prop = try replication.propagateCommand(allocator, handler.replicas.items, args, 2);
    handler.recordPropagation(prop.bytes_len);

    return CommandOutcome{
        .reply = reply,
        .notify = prop.ops,
    };
}

fn registerBlockingClient(handler: *AppHandler, client_connection: *ClientConnection, key: []const u8, timeout_secs: f64) !void {
    var gop = try handler.blocked_clients_by_key.getOrPut(key);
    if (!gop.found_existing) {
        gop.key_ptr.* = try handler.app_allocator.dupe(u8, key);
        gop.value_ptr.* = .{};
    }

    const NodeType = std.DoublyLinkedList(u64).Node;
    const node_ptr = try handler.app_allocator.create(NodeType);
    node_ptr.* = .{ .data = client_connection.id };
    gop.value_ptr.append(node_ptr);

    client_connection.*.blocking_key = gop.key_ptr.*;
    client_connection.*.blocking_node = node_ptr;

    if (timeout_secs > 0) {
        const now_us: i64 = std.time.microTimestamp();
        const delta_us: i64 = @intFromFloat(timeout_secs * @as(f64, @floatFromInt(std.time.us_per_s)));
        const deadline_us: i64 = now_us + delta_us;
        client_connection.*.deadline_us = deadline_us;

        try handler.timeouts.add(.{ .deadline_us = deadline_us, .connection_id = client_connection.id });
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

pub fn handleWait(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    client_connection: *ClientConnection,
    args: [64]?[]const u8,
) !union(enum) { Value: Reply, Blocked: struct { notify: ?[]WriteOp } } {
    if (argumentCount(args) != 3) {
        return .{ .Value = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } } };
    }

    const replicas_needed_slice = args[1].?;
    const timeout_slice = args[2].?;

    const replicas_needed = std.fmt.parseInt(i64, replicas_needed_slice, 10) catch return .{ .Value = Reply{ .Error = .{ .kind = ErrorKind.NotInteger } } };
    const timeout_ms = std.fmt.parseInt(i64, timeout_slice, 10) catch return .{ .Value = Reply{ .Error = .{ .kind = ErrorKind.NotInteger } } };

    if (replicas_needed < 0 or timeout_ms < 0) return .{ .Value = Reply{ .Error = .{ .kind = ErrorKind.NotInteger } } };

    const target_offset = handler.replication_offset;
    const required: u64 = @intCast(replicas_needed);

    var replica_ids_buffer = std.ArrayList(u64).init(allocator);
    defer replica_ids_buffer.deinit();

    if (handler.replicas.items.len == 0) {
        var it = handler.connection_by_id.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.is_replica) {
                try replica_ids_buffer.append(entry.value_ptr.id);
            }
        }
    }

    const replica_ids = if (handler.replicas.items.len != 0) handler.replicas.items else replica_ids_buffer.items;

    const total_replicas: u64 = @intCast(replica_ids.len);

    var acked_count: u64 = 0;
    for (replica_ids) |rid| {
        const ack = handler.replicaAck(rid);
        if (ack >= target_offset) {
            acked_count += 1;
        }
    }
    const acked = acked_count;

    if (acked >= required or total_replicas == 0 or acked == total_replicas or timeout_ms == 0) {
        return .{ .Value = Reply{ .Integer = @intCast(acked) } };
    }

    var args_buf: [64]?[]const u8 = undefined;
    @memset(args_buf[0..], null);
    args_buf[0] = "REPLCONF";
    args_buf[1] = "GETACK";
    args_buf[2] = "*";

    const prop = try replication.propagateCommand(allocator, replica_ids, args_buf, 3);
    handler.recordPropagation(prop.bytes_len);

    try handler.registerWait(client_connection, required, @intCast(timeout_ms));

    return .{ .Blocked = .{ .notify = prop.ops } };
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

pub fn handleIncr(handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const key = args[1] orelse return .{ .Error = .{ .kind = ErrorKind.ArgNum } };

    if (handler.list_store.contains(key)) return wrongTypeReply();
    if (handler.stream_store.contains(key)) return wrongTypeReply();

    const current = handler.string_store.getWithExpiration(key) orelse {
        const initial: i64 = 1;
        var buf: [32]u8 = undefined;
        const slice = try std.fmt.bufPrint(&buf, "{d}", .{initial});
        try handler.string_store.set(key, slice, null);
        return .{ .Integer = initial };
    };

    const parsed = std.fmt.parseInt(i64, current.data, 10) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };
    const incremented = std.math.add(i64, parsed, 1) catch return .{ .Error = .{ .kind = ErrorKind.NotInteger } };

    var buf: [32]u8 = undefined;
    const slice = try std.fmt.bufPrint(&buf, "{d}", .{incremented});
    try handler.string_store.set(key, slice, current.expiration_us);

    return .{ .Integer = incremented };
}

pub fn handleInfo(handler: *AppHandler, args: [64]?[]const u8) !Reply {
    _ = args;
    const info = if (handler.role == .replica) INFO_REPLICA else INFO_MASTER;
    return .{ .BulkString = info };
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

    const drain_result = try handler.drainWaitersForKey(allocator, key, pushed);

    var notify_list = std.ArrayList(WriteOp).init(allocator);
    errdefer notify_list.deinit();

    try notify_list.appendSlice(drain_result.ops);

    if (drain_result.popped > 0) {
        var lpop_args: [64]?[]const u8 = undefined;
        @memset(lpop_args[0..], null);
        lpop_args[0] = "LPOP";
        lpop_args[1] = key;

        var arg_count: usize = 2;
        var count_buf: [32]u8 = undefined;

        if (drain_result.popped > 1) {
            const count_str = try std.fmt.bufPrint(&count_buf, "{d}", .{drain_result.popped});
            lpop_args[2] = count_str;
            arg_count = 3;
        }

        const prop = try replication.propagateCommand(allocator, handler.replicas.items, lpop_args, arg_count);
        handler.recordPropagation(prop.bytes_len);
        try notify_list.appendSlice(prop.ops);
    }

    return .{
        .reply = Reply{ .Integer = @intCast(length) },
        .notify = try notify_list.toOwnedSlice(),
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

pub fn handleXadd(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !CommandOutcome {
    const key = args[1] orelse return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } }, .notify = &.{} };
    const entry_id = args[2] orelse return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } }, .notify = &.{} };

    if (handler.string_store.contains(key)) return .{ .reply = wrongTypeReply(), .notify = &.{} };
    if (handler.list_store.contains(key)) return .{ .reply = wrongTypeReply(), .notify = &.{} };

    const arg_count = argumentCount(args);
    if (arg_count < 5) return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } }, .notify = &.{} };
    const field_value_count = arg_count - 3;
    if ((field_value_count % 2) != 0) return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } }, .notify = &.{} };

    var pairs = std.ArrayList(db.StreamFieldValue).init(allocator);
    defer pairs.deinit();

    var index: usize = 3;
    while (index < arg_count) : (index += 2) {
        const field = args[index].?;
        const value = args[index + 1].?;
        try pairs.append(.{ .field = field, .value = value });
    }

    if (pairs.items.len == 0) return .{ .reply = Reply{ .Error = .{ .kind = ErrorKind.ArgNum } }, .notify = &.{} };

    const stored_entry_id = handler.stream_store.addEntry(allocator, key, entry_id, pairs.items) catch |err| {
        return .{ .reply = switch (err) {
            error.EntryIdTooSmall => Reply{ .Error = .{ .kind = ErrorKind.XaddIdTooSmall } },
            error.EntryIdZero => Reply{ .Error = .{ .kind = ErrorKind.XaddIdNotGreaterThanZero } },
            error.InvalidEntryId => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        }, .notify = &.{} };
    };

    const notify = try notifyStreamWaiters(allocator, handler, key);

    return .{
        .reply = Reply{ .BulkString = stored_entry_id },
        .notify = notify,
    };
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

pub fn handleXread(
    allocator: std.mem.Allocator,
    handler: *AppHandler,
    client_connection: *ClientConnection,
    args: [64]?[]const u8,
) !union(enum) { Value: Reply, Blocked } {
    const parsed = parseXreadArguments(allocator, args) catch |err| {
        return .{ .Value = switch (err) {
            error.ArgNum => Reply{ .Error = .{ .kind = ErrorKind.ArgNum } },
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            error.NotInteger => Reply{ .Error = .{ .kind = ErrorKind.NotInteger } },
            else => return err,
        } };
    };
    defer allocator.free(parsed.requests);

    var latest_id_allocs = std.ArrayList([]u8).init(allocator);
    defer {
        for (latest_id_allocs.items) |snapshot| {
            allocator.free(snapshot);
        }
        latest_id_allocs.deinit();
    }
    try resolveLatestStreamIds(allocator, handler, parsed.requests, &latest_id_allocs);

    const stream_replies = readStreamsIntoReplies(allocator, handler, parsed.requests) catch |err| {
        return .{ .Value = switch (err) {
            error.WrongType => wrongTypeReply(),
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        } };
    };

    if (stream_replies) |replies| {
        return .{ .Value = Reply{ .Array = replies } };
    }

    if (parsed.block_ms != null) {
        try handler.registerStreamBlockingClient(client_connection, parsed.requests, parsed.block_ms);
        return .Blocked;
    }

    return .{ .Value = Reply{ .Array = null } };
}

pub fn handleBlpop(allocator: std.mem.Allocator, handler: *AppHandler, client_connection: *ClientConnection, args: [64]?[]const u8) !union(enum) { Value: CommandOutcome, Blocked } {
    const request = parseBlpopRequest(args) catch |err| {
        return .{ .Value = .{ .reply = switch (err) {
            error.ArgNum => Reply{ .Error = .{ .kind = ErrorKind.ArgNum } },
            error.NotInteger => Reply{ .Error = .{ .kind = ErrorKind.NotInteger } },
            else => return err,
        }, .notify = &.{} } };
    };

    ensureListOperationAllowed(handler, request.key) catch |err| {
        return .{ .Value = .{ .reply = switch (err) {
            error.WrongType => wrongTypeReply(),
            else => return err,
        }, .notify = &.{} } };
    };

    if (try tryImmediateBlpop(allocator, handler, request.key)) |outcome| {
        return .{ .Value = outcome };
    }

    try registerBlockingClient(handler, client_connection, request.key, request.timeout_secs);
    return .Blocked;
}

pub fn handleReplconf(allocator: std.mem.Allocator, handler: *AppHandler, args: [64]?[]const u8) !Reply {
    const subcommand = args[1] orelse return Reply{ .Error = .{ .kind = ErrorKind.ArgNum } };

    if (std.ascii.eqlIgnoreCase(subcommand, "GETACK")) {
        var reply_args = try allocator.alloc(Reply, 3);
        reply_args[0] = Reply{ .BulkString = "REPLCONF" };
        reply_args[1] = Reply{ .BulkString = "ACK" };
        var offset_buf: [32]u8 = undefined;
        const offset_str = try std.fmt.bufPrint(&offset_buf, "{d}", .{handler.replication_offset});
        reply_args[2] = Reply{ .BulkString = try allocator.dupe(u8, offset_str) };
        return Reply{ .Array = reply_args };
    }

    return Reply{ .SimpleString = "OK" };
}

pub fn handlePsync(allocator: std.mem.Allocator, handler: *AppHandler, connection: *ClientConnection, args: [64]?[]const u8) !Reply {
    _ = args;

    const empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZWXCUIhuAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    const size = try std.base64.standard.Decoder.calcSizeForSlice(empty_rdb_base64);

    const rdb_bytes = try allocator.alloc(u8, size);

    try std.base64.standard.Decoder.decode(rdb_bytes, empty_rdb_base64);

    var buf = std.ArrayList(u8).init(allocator);

    try buf.appendSlice("+FULLRESYNC " ++ DEFAULT_REPL_ID ++ " 0\r\n");

    try buf.writer().print("${d}\r\n", .{rdb_bytes.len});

    try buf.appendSlice(rdb_bytes);

    if (!connection.is_replica) {
        connection.is_replica = true;
        try handler.replicas.append(connection.id);
        try handler.replica_acks.put(connection.id, 0);
    }

    return Reply{ .Bytes = try buf.toOwnedSlice() };
}
