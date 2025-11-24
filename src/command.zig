const std = @import("std");

pub const Command = enum {
    PING,
    ECHO,
    SET,
    GET,
    RPUSH,
    LRANGE,
    LPUSH,
    LLEN,
    LPOP,
    BLPOP,
    TYPE,
    XADD,
    XRANGE,
    XREAD,
    INCR,
    MULTI,
    DISCARD,
    EXEC,
    INFO,
    REPLCONF,
    PSYNC,
    WAIT,
    CONFIG,

    pub fn fromSlice(slice: []const u8) ?Command {
        if (std.ascii.eqlIgnoreCase(slice, "PING")) return .PING;
        if (std.ascii.eqlIgnoreCase(slice, "ECHO")) return .ECHO;
        if (std.ascii.eqlIgnoreCase(slice, "SET")) return .SET;
        if (std.ascii.eqlIgnoreCase(slice, "GET")) return .GET;
        if (std.ascii.eqlIgnoreCase(slice, "RPUSH")) return .RPUSH;
        if (std.ascii.eqlIgnoreCase(slice, "LRANGE")) return .LRANGE;
        if (std.ascii.eqlIgnoreCase(slice, "LPUSH")) return .LPUSH;
        if (std.ascii.eqlIgnoreCase(slice, "LLEN")) return .LLEN;
        if (std.ascii.eqlIgnoreCase(slice, "LPOP")) return .LPOP;
        if (std.ascii.eqlIgnoreCase(slice, "BLPOP")) return .BLPOP;
        if (std.ascii.eqlIgnoreCase(slice, "TYPE")) return .TYPE;
        if (std.ascii.eqlIgnoreCase(slice, "XADD")) return .XADD;
        if (std.ascii.eqlIgnoreCase(slice, "XRANGE")) return .XRANGE;
        if (std.ascii.eqlIgnoreCase(slice, "XREAD")) return .XREAD;
        if (std.ascii.eqlIgnoreCase(slice, "INCR")) return .INCR;
        if (std.ascii.eqlIgnoreCase(slice, "MULTI")) return .MULTI;
        if (std.ascii.eqlIgnoreCase(slice, "DISCARD")) return .DISCARD;
        if (std.ascii.eqlIgnoreCase(slice, "EXEC")) return .EXEC;
        if (std.ascii.eqlIgnoreCase(slice, "INFO")) return .INFO;
        if (std.ascii.eqlIgnoreCase(slice, "REPLCONF")) return .REPLCONF;
        if (std.ascii.eqlIgnoreCase(slice, "PSYNC")) return .PSYNC;
        if (std.ascii.eqlIgnoreCase(slice, "WAIT")) return .WAIT;
        if (std.ascii.eqlIgnoreCase(slice, "CONFIG")) return .CONFIG;
        return null;
    }
};
