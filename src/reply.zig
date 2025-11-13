pub const ErrorKind = enum {
    ArgNum,
    Syntax,
    NotInteger,
    WrongType,
    XaddIdTooSmall,
    XaddIdNotGreaterThanZero,
};

pub const Reply = union(enum) {
    SimpleString: []const u8,
    BulkString: ?[]const u8,
    Integer: i64,
    Array: ?[]const Reply,
    Error: struct { kind: ErrorKind },
};
