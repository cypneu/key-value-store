const std = @import("std");
const types = @import("types.zig");

pub const ListpackReader = struct {
    data: []const u8,
    pos: usize,

    const EOF = 0xFF;

    pub const Item = union(enum) {
        int: i64,
        string: []const u8,
    };

    pub fn init(data: []const u8) ListpackReader {
        if (data.len < 6) return .{ .data = data, .pos = 0 };
        return .{ .data = data, .pos = 6 };
    }

    pub fn isAtEnd(self: *ListpackReader) bool {
        return self.pos >= self.data.len or self.data[self.pos] == EOF;
    }

    pub fn next(self: *ListpackReader) !Item {
        if (self.pos >= self.data.len) return types.RdbError.UnexpectedEOF;

        const start_pos = self.pos;
        const b = self.data[self.pos];
        self.pos += 1;

        const item: Item = switch (b) {
            0x00...0x7F => .{ .int = @intCast(b) },
            0x80...0xBF => try self.readString6Bit(b),
            0xC0...0xDF => try self.readInt13Bit(b),
            0xE0...0xEF => try self.readString12Bit(b),
            0xF0 => try self.readString32Bit(),
            0xF1 => try self.readInt16Bit(),
            0xF2 => try self.readInt24Bit(),
            0xF3 => try self.readInt32Bit(),
            0xF4 => try self.readInt64Bit(),
            else => return types.RdbError.InvalidListpackEncoding,
        };

        const element_len = self.pos - start_pos;
        self.skipBacklen(element_len);

        return item;
    }

    pub fn readInt(self: *ListpackReader) !i64 {
        const item = try self.next();
        switch (item) {
            .int => |v| return v,
            .string => |s| return std.fmt.parseInt(i64, s, 10) catch 0,
        }
    }

    pub fn readStringOwned(self: *ListpackReader, allocator: std.mem.Allocator) ![]const u8 {
        const item = try self.next();
        switch (item) {
            .string => |s| return allocator.dupe(u8, s),
            .int => |v| return std.fmt.allocPrint(allocator, "{d}", .{v}),
        }
    }

    fn readString6Bit(self: *ListpackReader, b: u8) !Item {
        const len = b & 0x3F;
        return .{ .string = try self.readSlice(len) };
    }

    fn readInt13Bit(self: *ListpackReader, b: u8) !Item {
        if (self.pos >= self.data.len) return types.RdbError.UnexpectedEOF;
        const next_byte = self.data[self.pos];
        self.pos += 1;
        const val = (@as(u16, b & 0x1F) << 8) | next_byte;
        const shifted = @as(i16, @bitCast(val << 3));
        return .{ .int = @as(i64, shifted >> 3) };
    }

    fn readString12Bit(self: *ListpackReader, b: u8) !Item {
        const high = b & 0x0F;
        if (self.pos >= self.data.len) return types.RdbError.UnexpectedEOF;
        const low = self.data[self.pos];
        self.pos += 1;
        const len = (@as(usize, high) << 8) | low;
        return .{ .string = try self.readSlice(len) };
    }

    fn readString32Bit(self: *ListpackReader) !Item {
        const len = try self.readIntLittle(u32);
        return .{ .string = try self.readSlice(len) };
    }

    fn readInt16Bit(self: *ListpackReader) !Item {
        return .{ .int = try self.readIntLittle(i16) };
    }

    fn readInt24Bit(self: *ListpackReader) !Item {
        if (self.pos + 3 > self.data.len) return types.RdbError.UnexpectedEOF;
        const ptr = self.data[self.pos..];
        const v32 = @as(i32, ptr[0]) | (@as(i32, ptr[1]) << 8) | (@as(i32, ptr[2]) << 16);
        self.pos += 3;
        return .{ .int = (v32 << 8) >> 8 };
    }

    fn readInt32Bit(self: *ListpackReader) !Item {
        return .{ .int = try self.readIntLittle(i32) };
    }

    fn readInt64Bit(self: *ListpackReader) !Item {
        return .{ .int = try self.readIntLittle(i64) };
    }

    fn readSlice(self: *ListpackReader, len: usize) ![]const u8 {
        if (self.pos + len > self.data.len) return types.RdbError.UnexpectedEOF;
        const s = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return s;
    }

    fn readIntLittle(self: *ListpackReader, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.pos + size > self.data.len) return types.RdbError.UnexpectedEOF;
        const val = std.mem.readInt(T, self.data[self.pos..][0..size], .little);
        self.pos += size;
        return val;
    }

    fn skipBacklen(self: *ListpackReader, element_len: usize) void {
        if (element_len <= 127) {
            self.pos += 1;
        } else if (element_len < 16383) {
            self.pos += 2;
        } else if (element_len < 2097151) {
            self.pos += 3;
        } else if (element_len < 268435455) {
            self.pos += 4;
        } else {
            self.pos += 5;
        }
    }
};

test "listpack int 7bit" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0x05, 1, 0xFF };
    var lp = ListpackReader.init(&data);
    const val = try lp.readInt();
    try std.testing.expectEqual(@as(i64, 5), val);
}

test "listpack string 6bit" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0x85, 'h', 'e', 'l', 'l', 'o', 1, 0xFF };
    var lp = ListpackReader.init(&data);
    const val = try lp.readStringOwned(std.testing.allocator);
    defer std.testing.allocator.free(val);
    try std.testing.expectEqualStrings("hello", val);
}

test "listpack int 13bit" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0xC1, 0x05, 2, 0xFF };
    var lp = ListpackReader.init(&data);
    const val = try lp.readInt();
    try std.testing.expectEqual(@as(i64, 261), val);
}

test "listpack string 12bit" {
    var data = [_]u8{0} ** 300;
    @memcpy(data[0..6], &[_]u8{ 0, 0, 0, 0, 0, 0 });
    data[6] = 0xE0;
    data[7] = 10;
    @memset(data[8..18], 'a');
    data[18] = 1;
    data[19] = 0xFF;

    var lp = ListpackReader.init(data[0..20]);
    const val = try lp.readStringOwned(std.testing.allocator);
    defer std.testing.allocator.free(val);
    try std.testing.expectEqualStrings("aaaaaaaaaa", val);
}

test "listpack int 16bit" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0xF1, 0x20, 0x01, 3, 0xFF };
    var lp = ListpackReader.init(&data);
    const val = try lp.readInt();
    try std.testing.expectEqual(@as(i64, 288), val);
}

test "listpack int 32bit" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0xF3, 0x20, 0x00, 0x00, 0x00, 5, 0xFF };
    var lp = ListpackReader.init(&data);
    const val = try lp.readInt();
    try std.testing.expectEqual(@as(i64, 32), val);
}

test "listpack mixed" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0x05, 1, 0x83, 'f', 'o', 'o', 1, 0xFF };
    var lp = ListpackReader.init(&data);

    const v1 = try lp.readInt();
    try std.testing.expectEqual(@as(i64, 5), v1);

    const v2 = try lp.readStringOwned(std.testing.allocator);
    defer std.testing.allocator.free(v2);
    try std.testing.expectEqualStrings("foo", v2);
}
