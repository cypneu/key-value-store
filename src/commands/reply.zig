const std = @import("std");

pub const ErrorKind = enum {
    ArgNum,
    Syntax,
    NotInteger,
    WrongType,
    XaddIdTooSmall,
    XaddIdNotGreaterThanZero,
    ExecWithoutMulti,
    DiscardWithoutMulti,
    NestedMulti,
    ExecAbort,
    UnknownCommand,
    Loading,
};

pub const Reply = union(enum) {
    SimpleString: []const u8,
    BulkString: ?[]const u8,
    Integer: i64,
    Array: ?[]const Reply,
    Error: struct { kind: ErrorKind },
    Bytes: []const u8,
};

pub fn wrongTypeReply() Reply {
    return Reply{ .Error = .{ .kind = ErrorKind.WrongType } };
}

pub fn stringArrayReply(allocator: std.mem.Allocator, items: []const []const u8) !Reply {
    var arr = try allocator.alloc(Reply, items.len);
    for (items, 0..) |s, i| {
        arr[i] = Reply{ .BulkString = s };
    }
    return Reply{ .Array = arr };
}
