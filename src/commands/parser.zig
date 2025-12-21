const std = @import("std");

const command = @import("command.zig");
const StreamReadRequest = command.StreamReadRequest;
const XreadArgs = command.XreadArgs;
const CommandKind = command.CommandKind;
const CommandTag = command.CommandTag;
const Command = command.Command;
const ErrorKind = @import("reply.zig").ErrorKind;

const ParseResult = union(enum) {
    Command: Command,
    Error: ErrorKind,
};

const ParseKindResult = union(enum) {
    Kind: CommandKind,
    Error: ErrorKind,
};

pub fn parse(args: []const []const u8, raw: []const u8, allocator: std.mem.Allocator) !ParseResult {
    const kind_result = try parseKind(args, allocator);
    switch (kind_result) {
        .Error => |e| return .{ .Error = e },
        .Kind => |kind| {
            const owned_raw = try allocator.dupe(u8, raw);
            return .{ .Command = .{ .kind = kind, .raw = owned_raw } };
        },
    }
}

fn parseKind(args: []const []const u8, allocator: std.mem.Allocator) !ParseKindResult {
    if (args.len == 0) return .{ .Error = .ArgNum };

    const name = args[0];

    if (std.ascii.eqlIgnoreCase(name, "PING")) return .{ .Kind = .Ping };
    if (std.ascii.eqlIgnoreCase(name, "ECHO")) return .{ .Kind = .{ .Echo = .{ .value = if (args.len > 1) args[1] else null } } };
    if (std.ascii.eqlIgnoreCase(name, "GET")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .Get = .{ .key = key } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "TYPE")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .Type = .{ .key = key } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "SET")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const value = requireArg(args, 2) orelse break :blk .{ .Error = .ArgNum };
        var px_ms: ?u64 = null;

        if (args.len > 3 and std.ascii.eqlIgnoreCase(args[3], "PX")) {
            if (args.len < 5) break :blk .{ .Error = .Syntax };
            px_ms = std.fmt.parseInt(u64, args[4], 10) catch break :blk .{ .Error = .NotInteger };
        }

        break :blk .{ .Kind = .{ .Set = .{ .key = key, .value = value, .px_ms = px_ms } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "INCR")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .Incr = .{ .key = key } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "LPUSH")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const values = if (args.len > 2) args[2..] else &.{};
        break :blk .{ .Kind = .{ .Lpush = .{ .key = key, .values = values } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "RPUSH")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const values = if (args.len > 2) args[2..] else &.{};
        break :blk .{ .Kind = .{ .Rpush = .{ .key = key, .values = values } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "LRANGE")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const start_slice = requireArg(args, 2) orelse break :blk .{ .Error = .ArgNum };
        const end_slice = requireArg(args, 3) orelse break :blk .{ .Error = .ArgNum };

        const start = std.fmt.parseInt(i64, start_slice, 10) catch break :blk .{ .Error = .NotInteger };
        const end = std.fmt.parseInt(i64, end_slice, 10) catch break :blk .{ .Error = .NotInteger };

        break :blk .{ .Kind = .{ .Lrange = .{ .key = key, .start = start, .end = end } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "LLEN")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .Llen = .{ .key = key } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "LPOP")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const count = if (args.len > 2)
            std.fmt.parseInt(u64, args[2], 10) catch break :blk .{ .Error = .NotInteger }
        else
            1;
        break :blk .{ .Kind = .{ .Lpop = .{ .key = key, .count = count } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "BLPOP")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const timeout_slice = requireArg(args, 2) orelse break :blk .{ .Error = .ArgNum };
        const timeout_secs = std.fmt.parseFloat(f64, timeout_slice) catch break :blk .{ .Error = .NotInteger };
        if (timeout_secs < 0) break :blk .{ .Error = .NotInteger };
        break :blk .{ .Kind = .{ .Blpop = .{ .key = key, .timeout_secs = timeout_secs } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "XADD")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const entry_id = requireArg(args, 2) orelse break :blk .{ .Error = .ArgNum };
        if (args.len < 5) break :blk .{ .Error = .ArgNum };
        const fields = args[3..];
        if ((fields.len % 2) != 0) break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .Xadd = .{ .key = key, .entry_id = entry_id, .fields = fields } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "XRANGE")) return blk: {
        const key = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const start = requireArg(args, 2) orelse break :blk .{ .Error = .ArgNum };
        const end = requireArg(args, 3) orelse break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .Xrange = .{ .key = key, .start = start, .end = end } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "XREAD")) return blk: {
        if (args.len < 4) break :blk .{ .Error = .ArgNum };
        var idx: usize = 1;
        var block_ms: ?u64 = null;

        if (idx < args.len and std.ascii.eqlIgnoreCase(args[idx], "BLOCK")) {
            if (idx + 1 >= args.len) break :blk .{ .Error = .ArgNum };
            block_ms = std.fmt.parseInt(u64, args[idx + 1], 10) catch break :blk .{ .Error = .NotInteger };
            idx += 2;
        }

        const requests = parseXreadRequests(allocator, args, idx) catch |err| {
            break :blk .{ .Error = switch (err) {
                error.ArgNum => .ArgNum,
                error.Syntax => .Syntax,
                else => return err,
            } };
        };

        break :blk .{ .Kind = .{ .Xread = .{ .block_ms = block_ms, .requests = requests } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "MULTI")) return .{ .Kind = .Multi };
    if (std.ascii.eqlIgnoreCase(name, "DISCARD")) return .{ .Kind = .Discard };
    if (std.ascii.eqlIgnoreCase(name, "EXEC")) return .{ .Kind = .Exec };
    if (std.ascii.eqlIgnoreCase(name, "INFO")) return .{ .Kind = .Info };
    if (std.ascii.eqlIgnoreCase(name, "REPLCONF")) return blk: {
        const subcommand = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        const is_ack = std.ascii.eqlIgnoreCase(subcommand, "ACK");
        const ack_offset: ?u64 = if (is_ack) (std.fmt.parseInt(u64, requireArg(args, 2) orelse "0", 10) catch 0) else null;
        break :blk .{ .Kind = .{ .Replconf = .{
            .subcommand = subcommand,
            .is_getack = std.ascii.eqlIgnoreCase(subcommand, "GETACK"),
            .is_ack = is_ack,
            .ack_offset = ack_offset,
        } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "PSYNC")) return .{ .Kind = .Psync };
    if (std.ascii.eqlIgnoreCase(name, "WAIT")) return blk: {
        if (args.len != 3) break :blk .{ .Error = .ArgNum };
        const replicas_needed = std.fmt.parseInt(i64, args[1], 10) catch break :blk .{ .Error = .NotInteger };
        const timeout_ms = std.fmt.parseInt(i64, args[2], 10) catch break :blk .{ .Error = .NotInteger };
        if (replicas_needed < 0 or timeout_ms < 0) break :blk .{ .Error = .NotInteger };
        break :blk .{ .Kind = .{ .Wait = .{ .replicas_needed = @intCast(replicas_needed), .timeout_ms = @intCast(timeout_ms) } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "CONFIG")) return blk: {
        const subcommand = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        if (!std.ascii.eqlIgnoreCase(subcommand, "GET")) break :blk .{ .Error = .Syntax };
        const param = requireArg(args, 2) orelse break :blk .{ .Error = .ArgNum };
        break :blk .{ .Kind = .{ .ConfigGet = .{ .param = param } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "KEYS")) return blk: {
        const pattern = requireArg(args, 1) orelse break :blk .{ .Error = .ArgNum };
        if (!std.mem.eql(u8, pattern, "*")) break :blk .{ .Error = .Syntax };
        break :blk .{ .Kind = .{ .Keys = .{ .pattern = pattern } } };
    };
    if (std.ascii.eqlIgnoreCase(name, "SAVE")) return .{ .Kind = .Save };

    return .{ .Error = .UnknownCommand };
}

fn requireArg(args: []const []const u8, idx: usize) ?[]const u8 {
    if (idx >= args.len) return null;
    return args[idx];
}

fn parseXreadRequests(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    streams_index: usize,
) ![]StreamReadRequest {
    if (streams_index >= args.len) return error.ArgNum;

    const streams_token = args[streams_index];
    if (!std.ascii.eqlIgnoreCase(streams_token, "STREAMS")) {
        return error.Syntax;
    }

    const remaining = args.len - (streams_index + 1);
    if (remaining == 0 or (remaining % 2 != 0)) {
        return error.ArgNum;
    }

    const stream_count = remaining / 2;
    const keys_start: usize = streams_index + 1;
    const ids_start = keys_start + stream_count;

    const requests = try allocator.alloc(StreamReadRequest, stream_count);
    errdefer allocator.free(requests);

    for (requests, 0..) |*request, idx| {
        request.* = .{
            .key = args[keys_start + idx],
            .id = args[ids_start + idx],
        };
    }

    return requests;
}
