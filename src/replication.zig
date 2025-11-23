const std = @import("std");
const Command = @import("command.zig").Command;
const WriteOp = @import("state.zig").WriteOp;

pub fn isReplicationCommand(command: Command) bool {
    return switch (command) {
        .SET, .RPUSH, .LPUSH, .LPOP, .XADD, .INCR, .EXEC => true,
        else => false,
    };
}

pub fn propagateCommand(
    allocator: std.mem.Allocator,
    replicas: []const u64,
    command_parts: [64]?[]const u8,
    part_count: usize,
) ![]WriteOp {
    if (replicas.len == 0) return &.{};

    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    try buf.writer().print("*{d}\r\n", .{part_count});
    for (command_parts[0..part_count]) |part_opt| {
        const part = part_opt.?;
        try buf.writer().print("${d}\r\n{s}\r\n", .{ part.len, part });
    }

    const command_bytes = try buf.toOwnedSlice();
    errdefer allocator.free(command_bytes);

    var ops = try allocator.alloc(WriteOp, replicas.len);
    errdefer allocator.free(ops);

    for (replicas, 0..) |replica_id, i| {
        ops[i] = .{
            .connection_id = replica_id,
            .bytes = try allocator.dupe(u8, command_bytes),
        };
    }

    allocator.free(command_bytes);
    return ops;
}
