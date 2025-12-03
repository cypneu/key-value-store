const std = @import("std");
const types = @import("types.zig");

pub const Decompressor = struct {
    input: []const u8,
    output: []u8,
    in_pos: usize,
    out_pos: usize,

    const LITERAL_THRESHOLD = 32;

    pub fn init(input: []const u8, output: []u8) Decompressor {
        return .{
            .input = input,
            .output = output,
            .in_pos = 0,
            .out_pos = 0,
        };
    }

    pub fn decompress(self: *Decompressor) !void {
        while (self.in_pos < self.input.len) {
            try self.processBlock();
        }

        if (self.out_pos != self.output.len) {
            return types.RdbError.LzfSizeMismatch;
        }
    }

    fn processBlock(self: *Decompressor) !void {
        if (self.in_pos >= self.input.len) return types.RdbError.LzfInputTruncated;

        const ctrl = self.input[self.in_pos];
        self.in_pos += 1;

        if (ctrl < LITERAL_THRESHOLD) {
            try self.copyLiteral(ctrl);
        } else {
            try self.copyReference(ctrl);
        }
    }

    fn copyLiteral(self: *Decompressor, ctrl: u8) !void {
        const len = @as(usize, ctrl) + 1;

        if (self.in_pos + len > self.input.len) return types.RdbError.LzfInputTruncated;
        if (self.out_pos + len > self.output.len) return types.RdbError.LzfOutputTooSmall;

        @memcpy(self.output[self.out_pos .. self.out_pos + len], self.input[self.in_pos .. self.in_pos + len]);
        self.in_pos += len;
        self.out_pos += len;
    }

    fn copyReference(self: *Decompressor, ctrl: u8) !void {
        var len = @as(usize, ctrl >> 5);

        if (len == 7) {
            if (self.in_pos >= self.input.len) return types.RdbError.LzfInputTruncated;
            len += self.input[self.in_pos];
            self.in_pos += 1;
        }
        len += 2;

        if (self.in_pos >= self.input.len) return types.RdbError.LzfInputTruncated;
        const offset_low = self.input[self.in_pos];
        self.in_pos += 1;

        const offset_high = @as(usize, ctrl & 0x1F) << 8;
        const offset = offset_high | offset_low;

        if (offset >= self.out_pos) return types.RdbError.LzfInvalidReference;

        const ref_idx = self.out_pos - offset - 1;

        if (self.out_pos + len > self.output.len) return types.RdbError.LzfOutputTooSmall;

        var k: usize = 0;
        while (k < len) : (k += 1) {
            self.output[self.out_pos + k] = self.output[ref_idx + k];
        }
        self.out_pos += len;
    }
};

pub fn decompress(in: []const u8, out: []u8) !void {
    var decoder = Decompressor.init(in, out);
    try decoder.decompress();
}

test "lzf literal" {
    const in = [_]u8{ 0, 'h', 3, 'e', 'l', 'l', 'o' };
    var out: [5]u8 = undefined;
    try decompress(&in, &out);
    try std.testing.expectEqualStrings("hello", &out);
}

test "lzf reference simple" {
    const in = [_]u8{ 2, 'a', 'b', 'c', 32, 2 };
    var out: [6]u8 = undefined;
    try decompress(&in, &out);
    try std.testing.expectEqualStrings("abcabc", &out);
}

test "lzf reference overlap" {
    const in = [_]u8{ 0, 'a', 0x60, 0 };
    var out: [6]u8 = undefined;
    try decompress(&in, &out);
    try std.testing.expectEqualStrings("aaaaaa", &out);
}

test "lzf extended length" {
    const in = [_]u8{ 2, 'a', 'b', 'c', 0xE0, 1, 2 };
    var out: [13]u8 = undefined;
    try decompress(&in, &out);
    try std.testing.expectEqualStrings("abcabcabcabca", &out);
}

test "lzf error truncated" {
    const in = [_]u8{5};
    var out: [6]u8 = undefined;
    try std.testing.expectError(types.RdbError.LzfInputTruncated, decompress(&in, &out));
}

test "lzf error output too small" {
    const in = [_]u8{ 0, 'a' };
    var out: [0]u8 = undefined;
    try std.testing.expectError(types.RdbError.LzfOutputTooSmall, decompress(&in, &out));
}

