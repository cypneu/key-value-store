const std = @import("std");

const blocking = @import("../features/blocking.zig");
const connection_mod = @import("../network/connection.zig");
const Connection = connection_mod.Connection;
const writeReply = @import("../network/resp/writer.zig").writeReply;
const services_mod = @import("../services.zig");
const Services = services_mod.Services;
const WriteOp = services_mod.WriteOp;
const stream_store = @import("../storage/stream.zig");
const StreamEntry = stream_store.StreamEntry;
const StreamEntryField = stream_store.EntryField;
const StreamFieldValue = stream_store.FieldValue;
const command = @import("command.zig");
const StreamReadRequest = command.StreamReadRequest;
const XreadArgs = command.XreadArgs;
const dispatcher = @import("dispatcher.zig");
const CommandOutcome = dispatcher.CommandOutcome;
const Output = dispatcher.Output;
const reply_mod = @import("reply.zig");
const Reply = reply_mod.Reply;
const ErrorKind = reply_mod.ErrorKind;
const wrongTypeReply = reply_mod.wrongTypeReply;

pub fn handleXadd(
    allocator: std.mem.Allocator,
    services: *Services,
    key: []const u8,
    entry_id: []const u8,
    fields: []const []const u8,
) !Output {
    if (services.string_store.contains(key) or services.list_store.contains(key)) {
        return .{ .reply = wrongTypeReply() };
    }

    var pairs = std.ArrayList(StreamFieldValue).init(allocator);
    defer pairs.deinit();

    var index: usize = 0;
    while (index < fields.len) : (index += 2) {
        const field = fields[index];
        const value = fields[index + 1];
        try pairs.append(.{ .field = field, .value = value });
    }

    const stored_entry_id = services.stream_store.addEntry(allocator, key, entry_id, pairs.items) catch |err| {
        const r: Reply = switch (err) {
            error.EntryIdTooSmall => Reply{ .Error = .{ .kind = ErrorKind.XaddIdTooSmall } },
            error.EntryIdZero => Reply{ .Error = .{ .kind = ErrorKind.XaddIdNotGreaterThanZero } },
            error.InvalidEntryId => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
        return .{ .reply = r };
    };

    const stream_notify = try notifyStreamWaiters(allocator, services, key);

    return .{
        .reply = Reply{ .BulkString = stored_entry_id },
        .side_effects = stream_notify,
    };
}

pub fn handleXrange(allocator: std.mem.Allocator, services: *Services, key: []const u8, start_id: []const u8, end_id: []const u8) !Reply {
    if (services.string_store.contains(key)) return wrongTypeReply();
    if (services.list_store.contains(key)) return wrongTypeReply();

    const entries = services.stream_store.xrange(allocator, key, start_id, end_id) catch |err| {
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
    services: *Services,
    client_connection: *Connection,
    parsed: XreadArgs,
) !CommandOutcome {
    var latest_id_allocs = std.ArrayList([]u8).init(allocator);
    defer {
        for (latest_id_allocs.items) |snapshot| {
            allocator.free(snapshot);
        }
        latest_id_allocs.deinit();
    }
    try resolveLatestStreamIds(allocator, services, parsed.requests, &latest_id_allocs);

    const stream_replies = readStreamsIntoReplies(allocator, services, parsed.requests) catch |err| {
        const r: Reply = switch (err) {
            error.WrongType => wrongTypeReply(),
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
        return .{ .Immediate = .{ .reply = r } };
    };

    if (stream_replies) |replies| {
        return .{ .Immediate = .{ .reply = Reply{ .Array = replies } } };
    }

    if (parsed.block_ms != null) {
        try services.blocking.registerStreamBlockingClient(client_connection.id, parsed.requests, parsed.block_ms);
        return .{ .Blocked = .{} };
    }

    return .{ .Immediate = .{ .reply = Reply{ .Array = null } } };
}

pub fn handleXreadNonBlocking(
    allocator: std.mem.Allocator,
    services: *Services,
    client_connection: *Connection,
    parsed: XreadArgs,
) !CommandOutcome {
    const adjusted = XreadArgs{ .block_ms = null, .requests = parsed.requests };
    return handleXread(allocator, services, client_connection, adjusted);
}

fn streamEntryFieldsReply(allocator: std.mem.Allocator, fields: []const StreamEntryField) !Reply {
    const total = fields.len * 2;
    var arr = try allocator.alloc(Reply, total);

    for (fields, 0..) |field, i| {
        const base = i * 2;
        arr[base] = Reply{ .BulkString = field.name };
        arr[base + 1] = Reply{ .BulkString = field.value };
    }

    return Reply{ .Array = arr };
}

fn streamEntriesReply(allocator: std.mem.Allocator, entries: []*const StreamEntry) ![]Reply {
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

fn streamReadResultReply(allocator: std.mem.Allocator, key: []const u8, entries: []*const StreamEntry) !Reply {
    const entry_reply = try streamEntriesReply(allocator, entries);
    var stream_pair = try allocator.alloc(Reply, 2);
    stream_pair[0] = Reply{ .BulkString = try allocator.dupe(u8, key) };
    stream_pair[1] = Reply{ .Array = entry_reply };
    return Reply{ .Array = stream_pair };
}

fn isLatestStreamId(id: []const u8) bool {
    return id.len == 1 and id[0] == '$';
}

fn latestStreamIdSnapshot(allocator: std.mem.Allocator, services: *Services, key: []const u8) ![]u8 {
    var id_buffer: [64]u8 = undefined;
    if (try services.stream_store.formatLastEntryId(key, id_buffer[0..])) |formatted| {
        return try allocator.dupe(u8, formatted);
    }
    return try allocator.dupe(u8, "0-0");
}

fn resolveLatestStreamIds(
    allocator: std.mem.Allocator,
    services: *Services,
    requests: []StreamReadRequest,
    replacements: *std.ArrayList([]u8),
) !void {
    for (requests, 0..) |request, idx| {
        if (!isLatestStreamId(request.id)) continue;
        const snapshot = try latestStreamIdSnapshot(allocator, services, request.key);
        try replacements.append(snapshot);
        requests[idx].id = snapshot;
    }
}

fn readStreamsIntoReplies(allocator: std.mem.Allocator, services: *Services, requests: []const StreamReadRequest) !?[]Reply {
    var streams_reply = std.ArrayList(Reply).init(allocator);
    defer streams_reply.deinit();

    for (requests) |req| {
        if (services.string_store.contains(req.key) or services.list_store.contains(req.key)) {
            return error.WrongType;
        }

        const entries = services.stream_store.xread(allocator, req.key, req.id) catch |err| {
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
    try writeReply(buf.writer(), reply);
    return try buf.toOwnedSlice();
}

fn tryResumeXreadConnection(
    allocator: std.mem.Allocator,
    services: *Services,
    connection: *Connection,
) !?Reply {
    const blocked_state = services.blocking.blocked_clients.get(connection.id) orelse return null;
    const block_state = switch (blocked_state) {
        .Stream => |s| s,
        else => return null,
    };

    const stream_replies = readStreamsIntoReplies(allocator, services, block_state.requests) catch |err| {
        services.blocking.clearStreamState(connection.id);
        return switch (err) {
            error.WrongType => wrongTypeReply(),
            error.Syntax => Reply{ .Error = .{ .kind = ErrorKind.Syntax } },
            else => return err,
        };
    };

    if (stream_replies == null) return null;

    services.blocking.clearStreamState(connection.id);
    return Reply{ .Array = stream_replies };
}

fn notifyStreamWaiters(allocator: std.mem.Allocator, services: *Services, key: []const u8) ![]WriteOp {
    const wait_list_ptr = services.blocking.blocked_stream_clients_by_key.getPtr(key) orelse return &.{};

    var notifications = std.ArrayList(WriteOp).init(allocator);
    errdefer notifications.deinit();

    var node_ptr_opt = wait_list_ptr.first;
    while (node_ptr_opt) |node_ptr| {
        const next = node_ptr.next;

        const id = node_ptr.data;
        if (services.connections.get(id)) |connection| {
            if (try tryResumeXreadConnection(allocator, services, connection)) |r| {
                try notifications.append(.{ .connection_id = id, .reply = r });
            }
        } else {
            wait_list_ptr.remove(node_ptr);
            services.allocator.destroy(node_ptr);
        }

        node_ptr_opt = next;
    }

    if (notifications.items.len == 0) {
        return &.{};
    }
    return try notifications.toOwnedSlice();
}
