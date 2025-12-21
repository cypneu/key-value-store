const std = @import("std");

pub const StreamReadRequest = struct {
    key: []const u8,
    id: []const u8,
};

pub const XreadArgs = struct {
    block_ms: ?u64,
    requests: []StreamReadRequest,
};

pub const CommandKind = union(enum) {
    Ping,
    Echo: struct { value: ?[]const u8 },
    Set: struct { key: []const u8, value: []const u8, px_ms: ?u64 },
    Get: struct { key: []const u8 },
    Rpush: struct { key: []const u8, values: []const []const u8 },
    Lrange: struct { key: []const u8, start: i64, end: i64 },
    Lpush: struct { key: []const u8, values: []const []const u8 },
    Llen: struct { key: []const u8 },
    Lpop: struct { key: []const u8, count: u64 },
    Blpop: struct { key: []const u8, timeout_secs: f64 },
    Type: struct { key: []const u8 },
    Xadd: struct { key: []const u8, entry_id: []const u8, fields: []const []const u8 },
    Xrange: struct { key: []const u8, start: []const u8, end: []const u8 },
    Xread: XreadArgs,
    Incr: struct { key: []const u8 },
    Multi,
    Discard,
    Exec,
    Info,
    Replconf: struct { subcommand: []const u8, is_getack: bool, is_ack: bool, ack_offset: ?u64 },
    Psync,
    Wait: struct { replicas_needed: u64, timeout_ms: u64 },
    ConfigGet: struct { param: []const u8 },
    Keys: struct { pattern: []const u8 },
    Save,
};

pub const CommandTag = std.meta.Tag(CommandKind);

pub const Command = struct {
    kind: CommandKind,
    raw: []const u8,

    pub fn tag(self: Command) CommandTag {
        return std.meta.activeTag(self.kind);
    }
};

pub fn isReplicationCommand(t: CommandTag) bool {
    return switch (t) {
        .Set, .Rpush, .Lpush, .Lpop, .Xadd, .Incr, .Exec => true,
        else => false,
    };
}

pub fn isTransactionControl(t: CommandTag) bool {
    return switch (t) {
        .Multi, .Discard, .Exec => true,
        else => false,
    };
}

pub fn isReplconfGetack(kind: CommandKind) bool {
    return switch (kind) {
        .Replconf => |data| data.is_getack,
        else => false,
    };
}
