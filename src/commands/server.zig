const std = @import("std");

const replication = @import("../features/replication.zig");
const connection_mod = @import("../network/connection.zig");
const Connection = connection_mod.Connection;
const services_mod = @import("../services.zig");
const Services = services_mod.Services;
const WriteOp = services_mod.WriteOp;
const rdb_writer = @import("../storage/rdb/writer.zig");
const command = @import("command.zig");
const dispatcher = @import("dispatcher.zig");
const CommandOutcome = dispatcher.CommandOutcome;
const Output = dispatcher.Output;
const reply_mod = @import("reply.zig");
const Reply = reply_mod.Reply;
const stringArrayReply = reply_mod.stringArrayReply;

const Replconf = std.meta.TagPayload(command.CommandKind, .Replconf);

const DEFAULT_REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

pub fn handleInfo(allocator: std.mem.Allocator, services: *Services) !Reply {
    const role_str = if (services.replication.role == .replica) "slave" else "master";
    const offset = services.replication.offset;

    const info = try std.fmt.allocPrint(allocator, "role:{s}\r\nmaster_replid:" ++ DEFAULT_REPL_ID ++ "\r\nmaster_repl_offset:{d}\r\n", .{ role_str, offset });
    return .{ .BulkString = info };
}

pub fn handleWait(
    allocator: std.mem.Allocator,
    services: *Services,
    client_connection: *Connection,
    replicas_needed: u64,
    timeout_ms: u64,
) !CommandOutcome {
    const target_offset = services.replication.offset;
    const required: u64 = replicas_needed;

    var replica_ids_buffer = std.ArrayList(u64).init(allocator);
    defer replica_ids_buffer.deinit();

    if (services.replication.replicas.items.len == 0) {
        var it = services.connections.iterator();
        while (it.next()) |entry| {
            if (services.replication.isReplica(entry.value_ptr.id)) {
                try replica_ids_buffer.append(entry.value_ptr.id);
            }
        }
    }

    const replica_ids = if (services.replication.replicas.items.len != 0) services.replication.replicas.items else replica_ids_buffer.items;

    var acked_count: u64 = 0;
    for (replica_ids) |rid| {
        const ack = services.replication.getAck(rid);
        if (ack >= target_offset) {
            acked_count += 1;
        }
    }
    const acked = acked_count;

    if (acked >= required or timeout_ms == 0) {
        return .{ .Immediate = .{ .reply = Reply{ .Integer = @intCast(acked) } } };
    }

    const args_buf = [_][]const u8{ "REPLCONF", "GETACK", "*" };
    const prop = try replication.propagateCommand(allocator, replica_ids, args_buf[0..]);
    services.replication.recordOffset(prop.bytes_len);

    try services.replication.registerWait(&services.blocking, client_connection.id, required, timeout_ms, target_offset);

    return .{ .Blocked = .{ .replies = prop.ops } };
}

pub fn handleReplconf(allocator: std.mem.Allocator, services: *Services, connection_id: u64, data: Replconf) !Output {
    if (data.is_ack) {
        const notify = try services.replication.recordReplicaAckAndBuildNotifications(&services.blocking, allocator, connection_id, data.ack_offset orelse 0);
        return .{ .side_effects = notify orelse &.{} };
    }

    if (data.is_getack) {
        var reply_args = try allocator.alloc(Reply, 3);
        reply_args[0] = Reply{ .BulkString = "REPLCONF" };
        reply_args[1] = Reply{ .BulkString = "ACK" };
        var offset_buf: [32]u8 = undefined;
        const offset_str = try std.fmt.bufPrint(&offset_buf, "{d}", .{services.replication.offset});
        reply_args[2] = Reply{ .BulkString = try allocator.dupe(u8, offset_str) };
        return .{ .reply = Reply{ .Array = reply_args } };
    }

    return .{ .reply = Reply{ .SimpleString = "OK" } };
}

pub fn handlePsync(allocator: std.mem.Allocator, services: *Services, connection: *Connection) !Output {
    if (!services.replication.isReplica(connection.id)) {
        try services.replication.markAsReplica(connection.id);
        try services.replication.registerReplica(connection.id);
    }

    const maybe_snapshot = try services.replication.prepareSnapshot(&services.string_store, &services.list_store, &services.stream_store, connection.id);

    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    const repl_offset = services.replication.offset;
    try buf.writer().print("+FULLRESYNC " ++ DEFAULT_REPL_ID ++ " {d}\r\n", .{repl_offset});

    return .{
        .reply = Reply{ .Bytes = try buf.toOwnedSlice() },
        .snapshot = maybe_snapshot,
    };
}

pub fn handleConfig(allocator: std.mem.Allocator, services: *Services, param: []const u8) !Reply {
    if (std.ascii.eqlIgnoreCase(param, "dir")) {
        var arr = try allocator.alloc(Reply, 2);
        arr[0] = Reply{ .BulkString = "dir" };
        arr[1] = Reply{ .BulkString = services.config.dir };
        return Reply{ .Array = arr };
    } else if (std.ascii.eqlIgnoreCase(param, "dbfilename")) {
        var arr = try allocator.alloc(Reply, 2);
        arr[0] = Reply{ .BulkString = "dbfilename" };
        arr[1] = Reply{ .BulkString = services.config.db_filename };
        return Reply{ .Array = arr };
    }

    return Reply{ .Array = &[_]Reply{} };
}

pub fn handleKeys(allocator: std.mem.Allocator, services: *Services) !Reply {
    const string_keys = try services.string_store.keys(allocator);
    defer allocator.free(string_keys);

    const list_keys = try services.list_store.keys(allocator);
    defer allocator.free(list_keys);

    const stream_keys = try services.stream_store.keys(allocator);
    defer allocator.free(stream_keys);

    const total = string_keys.len + list_keys.len + stream_keys.len;
    var merged = try allocator.alloc([]const u8, total);
    defer allocator.free(merged);

    var idx: usize = 0;
    for (string_keys) |k| {
        merged[idx] = k;
        idx += 1;
    }
    for (list_keys) |k| {
        merged[idx] = k;
        idx += 1;
    }
    for (stream_keys) |k| {
        merged[idx] = k;
        idx += 1;
    }

    return try stringArrayReply(allocator, merged);
}

pub fn handleSave(allocator: std.mem.Allocator, services: *Services) !Reply {
    const tmp_filename = try std.fmt.allocPrint(allocator, "temp-{d}.rdb", .{std.time.milliTimestamp()});
    defer allocator.free(tmp_filename);

    const tmp_path = try std.fs.path.join(allocator, &[_][]const u8{ services.config.dir, tmp_filename });
    defer allocator.free(tmp_path);

    const target_path = try std.fs.path.join(allocator, &[_][]const u8{ services.config.dir, services.config.db_filename });
    defer allocator.free(target_path);

    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();
        var buf_writer = std.io.bufferedWriter(file.writer());
        try rdb_writer.writeSnapshot(buf_writer.writer(), allocator, &services.string_store, &services.list_store, &services.stream_store);
        try buf_writer.flush();
    }

    try std.fs.cwd().rename(tmp_path, target_path);

    return Reply{ .SimpleString = "OK" };
}
