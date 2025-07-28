const std = @import("std");

pub const RESP = struct {
    const Reader = std.io.FixedBufferStream([]const u8).Reader;

    const BulkString = []const u8;
    const Array = [64]?BulkString;

    pub const Error = error{
        NotArray,
        MissingArraySize,
        MissingCLRF,
    };

    pub fn parse(input: []const u8) Error!Array {
        var stream = std.io.fixedBufferStream(input);
        var reader = stream.reader();
        return try parseArray(&reader);
    }

    fn parseArray(reader: *Reader) Error!Array {
        const first = reader.*.readByte() catch return Error.NotArray;
        if (first != '*') return Error.NotArray;

        const array_len = try readLength(reader);

        var array_buffer: [64]?BulkString = .{null} ** 64;
        for (0..array_len) |index| {
            const bulk_string_value = try parseBulkString(reader);
            array_buffer[index] = bulk_string_value;
        }

        return array_buffer;
    }

    fn parseBulkString(reader: *Reader) Error!BulkString {
        const first_byte = reader.*.readByte() catch return Error.NotArray;
        if (first_byte != '$') return Error.NotArray;

        _ = try readLength(reader);
        return try readLineSlice(reader);
    }

    fn readLineSlice(reader: *Reader) Error![]const u8 {
        const ctx = reader.context;
        const start = ctx.pos;
        while (ctx.pos < ctx.buffer.len) : (ctx.pos += 1) {
            if (ctx.buffer[ctx.pos] == '\r') {
                const end = ctx.pos;
                ctx.pos += 1;
                try expectLF(reader);
                return ctx.buffer[start..end];
            }
        }
        return Error.MissingCLRF;
    }

    fn readIntLine(reader: *Reader) Error!u64 {
        const line = try readLineSlice(reader);
        return std.fmt.parseInt(u64, line, 10) catch return Error.MissingArraySize;
    }

    fn readLength(reader: *Reader) Error!u64 {
        return try readIntLine(reader);
    }

    fn expectLF(reader: *Reader) Error!void {
        const lf = reader.*.readByte() catch return Error.MissingCLRF;
        if (lf != '\n') return Error.MissingCLRF;
    }
};

test "parse PING command" {}

test "parse ECHO command" {}
