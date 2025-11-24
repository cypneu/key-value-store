const std = @import("std");
const AppHandler = @import("state.zig").AppHandler;

pub const RdbError = error{
    InvalidHeader,
    InvalidChecksum,
    UnsupportedEncoding,
    UnexpectedEOF,
    InvalidLengthEncoding,
};

pub fn loadRDB(allocator: std.mem.Allocator, dir: []const u8, filename: []const u8, handler: *AppHandler) !void {
    const path = try std.fs.path.join(allocator, &[_][]const u8{ dir, filename });
    defer allocator.free(path);

    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        if (err == error.FileNotFound) return;
        return err;
    };
    defer file.close();

    var buf_reader = std.io.bufferedReader(file.reader());
    var loader = RdbLoader(@TypeOf(buf_reader.reader())).init(allocator, buf_reader.reader(), handler);
    try loader.load();
}

fn RdbLoader(comptime Reader: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: Reader,
        handler: *AppHandler,
        peeked_byte: ?u8 = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: Reader, handler: *AppHandler) Self {
            return .{
                .allocator = allocator,
                .reader = reader,
                .handler = handler,
            };
        }

        pub fn load(self: *Self) !void {
            try self.verifyHeader();

            while (true) {
                const byte = self.readByte() catch |err| {
                    if (err == error.EndOfStream) return;
                    return err;
                };

                switch (byte) {
                    0xFA => try self.parseMetadata(),
                    0xFE => try self.parseDatabase(),
                    0xFF => return, // EOF
                    else => {
                        // Ignore unknown top-level bytes
                    },
                }
            }
        }

        fn verifyHeader(self: *Self) !void {
            var header: [9]u8 = undefined;
            const bytes_read = try self.reader.readAll(&header);
            if (bytes_read != 9 or !std.mem.eql(u8, header[0..5], "REDIS")) {
                return RdbError.InvalidHeader;
            }
        }

        fn parseMetadata(self: *Self) !void {
            const key = try self.readString();
            defer self.allocator.free(key);
            const val = try self.readString();
            defer self.allocator.free(val);
        }

        fn parseDatabase(self: *Self) !void {
            _ = try self.readLength(); // Database index

            const byte = try self.readByte();
            if (byte == 0xFB) {
                _ = try self.readLength(); // hash table size
                _ = try self.readLength(); // expire hash table size
            } else {
                self.peeked_byte = byte;
            }

            while (try self.parseKeyValuePair()) {}
        }

        fn parseKeyValuePair(self: *Self) !bool {
            const opcode = try self.readByte();

            switch (opcode) {
                0xFF => { // EOF
                    self.peeked_byte = opcode;
                    return false;
                },
                0xFE => { // Start of next database
                    self.peeked_byte = opcode;
                    return false;
                },
                0xFD => return try self.parseKeyWithExpire(.Seconds),
                0xFC => return try self.parseKeyWithExpire(.Milliseconds),
                else => return try self.parseEntry(opcode, null),
            }
        }

        const ExpireFormat = enum { Seconds, Milliseconds };

        fn parseKeyWithExpire(self: *Self, format: ExpireFormat) !bool {
            const expire_ms = switch (format) {
                .Seconds => @as(u64, try self.reader.readInt(u32, .little)) * 1000,
                .Milliseconds => try self.reader.readInt(u64, .little),
            };

            const value_type = try self.readByte();
            return try self.parseEntry(value_type, expire_ms);
        }

        fn parseEntry(self: *Self, value_type: u8, expire_ms: ?u64) !bool {
            if (value_type != 0) return RdbError.UnsupportedEncoding;

            const key = try self.readString();
            errdefer self.allocator.free(key);

            const val = try self.readString();
            errdefer self.allocator.free(val);

            var expire_us: ?i64 = null;
            if (expire_ms) |ms| {
                expire_us = @as(i64, @intCast(ms)) * 1000;
            }

            try self.handler.string_store.set(key, val, expire_us);
            self.allocator.free(key);
            self.allocator.free(val);

            return true;
        }

        fn readByte(self: *Self) !u8 {
            if (self.peeked_byte) |b| {
                self.peeked_byte = null;
                return b;
            }
            return self.reader.readByte();
        }

        fn readLength(self: *Self) !struct { len: u64, is_encoded: bool, encoding_type: u6 } {
            const first_byte = try self.readByte();
            const enc_type = (first_byte & 0xC0) >> 6;

            switch (enc_type) {
                0b00 => return .{ .len = first_byte & 0x3F, .is_encoded = false, .encoding_type = 0 },
                0b01 => {
                    const next_byte = try self.readByte();
                    const len = (@as(u64, first_byte & 0x3F) << 8) | next_byte;
                    return .{ .len = len, .is_encoded = false, .encoding_type = 0 };
                },
                0b10 => {
                    const len = try self.reader.readInt(u32, .big);
                    return .{ .len = len, .is_encoded = false, .encoding_type = 0 };
                },
                0b11 => return .{ .len = 0, .is_encoded = true, .encoding_type = @truncate(first_byte & 0x3F) },
                else => unreachable,
            }
        }

        fn readString(self: *Self) ![]u8 {
            const length_info = try self.readLength();

            if (length_info.is_encoded) {
                switch (length_info.encoding_type) {
                    0 => {
                        const val = try self.readByte();
                        return try std.fmt.allocPrint(self.allocator, "{d}", .{val});
                    },
                    1 => {
                        const val = try self.reader.readInt(u16, .little);
                        return try std.fmt.allocPrint(self.allocator, "{d}", .{val});
                    },
                    2 => {
                        const val = try self.reader.readInt(u32, .little);
                        return try std.fmt.allocPrint(self.allocator, "{d}", .{val});
                    },
                    3 => return RdbError.UnsupportedEncoding,
                    else => return RdbError.UnsupportedEncoding,
                }
            } else {
                const len = length_info.len;
                const buf = try self.allocator.alloc(u8, len);
                errdefer self.allocator.free(buf);
                try self.reader.readNoEof(buf);
                return buf;
            }
        }
    };
}

