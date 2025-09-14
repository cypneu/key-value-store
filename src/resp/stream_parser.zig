const std = @import("std");

pub const StreamParser = struct {
    const max_parts = 64;

    pub const Error = error{
        NotArray,
        MissingArraySize,
        MissingCLRF,
        TooManyArgs,
        Overflow,
    };

    pub const ParseResult = union(enum) {
        NeedMore,
        Done: struct { consumed: usize, parts: [max_parts]?[]const u8 },
        Error: Error,
    };

    const State = enum {
        ExpectArrayStart,
        ArrayLenDigits,
        ExpectLFArrayLen,
        ExpectArgOrDone,
        ExpectBulkStart,
        BulkLenDigits,
        ExpectLFBulkLen,
        BulkData,
        BulkCR,
        BulkLF,
    };

    state: State = .ExpectArrayStart,
    cursor: usize = 0,

    array_len: usize = 0,
    args_idx: [max_parts]?PartIndex = .{null} ** max_parts,
    args_count: usize = 0,

    num_acc: u64 = 0,
    num_had_digit: bool = false,

    bulk_remaining: usize = 0,

    const PartIndex = struct { start: usize, len: usize };

    pub fn parse(self: *StreamParser, buf: []const u8) ParseResult {
        var i: usize = self.cursor;

        while (true) {
            const outcome = switch (self.state) {
                .ExpectArrayStart => self.handleExpectArrayStart(buf, &i),
                .ArrayLenDigits => self.handleArrayLenDigits(buf, &i),
                .ExpectLFArrayLen => self.handleExpectLfArrayLen(buf, &i),
                .ExpectArgOrDone => self.handleExpectArgOrDone(buf, &i),
                .ExpectBulkStart => self.handleExpectBulkStart(buf, &i),
                .BulkLenDigits => self.handleBulkLenDigits(buf, &i),
                .ExpectLFBulkLen => self.handleExpectLfBulkLen(buf, &i),
                .BulkData => self.handleBulkData(buf, &i),
                .BulkCR => self.handleBulkCr(buf, &i),
                .BulkLF => self.handleBulkLf(buf, &i),
            };

            if (outcome) |result| return result;
        }
    }

    fn handleExpectArrayStart(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        const prefix = buf[i];
        if (prefix != '*') return .{ .Error = Error.NotArray };

        i += 1;
        self.startNumber();
        self.state = .ArrayLenDigits;
        idx_ptr.* = i;
        return null;
    }

    fn handleArrayLenDigits(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        while (true) {
            if (i >= buf.len) return self.needMore(i);

            const b = buf[i];
            if (b >= '0' and b <= '9') {
                self.num_had_digit = true;
                const digit: u64 = @intCast(b - '0');
                const next = self.num_acc * 10 + digit;
                if (next < self.num_acc) return .{ .Error = Error.Overflow };
                self.num_acc = next;
                i += 1;
                continue;
            }

            if (b == '\r' and self.num_had_digit) {
                i += 1;
                self.state = .ExpectLFArrayLen;
                idx_ptr.* = i;
                return null;
            }

            return .{ .Error = Error.MissingArraySize };
        }
    }

    fn handleExpectLfArrayLen(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '\n') return .{ .Error = Error.MissingCLRF };
        i += 1;

        if (self.num_acc > max_parts) return .{ .Error = Error.TooManyArgs };
        self.array_len = @intCast(self.num_acc);
        self.clearArgState();
        self.state = .ExpectArgOrDone;

        idx_ptr.* = i;
        return null;
    }

    fn handleExpectArgOrDone(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        if (self.array_len == 0) {
            return self.finishMessage(buf, idx_ptr.*);
        }

        self.state = .ExpectBulkStart;
        return null;
    }

    fn handleExpectBulkStart(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '$') return .{ .Error = Error.NotArray };

        i += 1;
        self.startNumber();
        self.state = .BulkLenDigits;
        idx_ptr.* = i;
        return null;
    }

    fn handleBulkLenDigits(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        while (true) {
            if (i >= buf.len) return self.needMore(i);

            const b = buf[i];
            if (b >= '0' and b <= '9') {
                self.num_had_digit = true;
                const digit: u64 = @intCast(b - '0');
                const next = self.num_acc * 10 + digit;
                if (next < self.num_acc) return .{ .Error = Error.Overflow };
                self.num_acc = next;
                i += 1;
                continue;
            }

            if (b == '\r' and self.num_had_digit) {
                i += 1;
                self.state = .ExpectLFBulkLen;
                idx_ptr.* = i;
                return null;
            }

            return .{ .Error = Error.MissingArraySize };
        }
    }

    fn handleExpectLfBulkLen(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '\n') return .{ .Error = Error.MissingCLRF };
        i += 1;

        self.bulk_remaining = @intCast(self.num_acc);
        self.state = .BulkData;
        idx_ptr.* = i;
        return null;
    }

    fn handleBulkData(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;

        if (self.bulk_remaining == 0) {
            const start = i;
            self.args_idx[self.args_count] = PartIndex{ .start = start, .len = 0 };
            self.bulk_remaining = 0;
            self.state = .BulkCR;
            idx_ptr.* = i;
            return null;
        }

        const available = buf.len - i;
        if (available < self.bulk_remaining) return self.needMore(i);

        const start = i;
        const end = i + self.bulk_remaining;
        self.args_idx[self.args_count] = PartIndex{ .start = start, .len = end - start };
        i = end;
        self.bulk_remaining = 0;
        self.state = .BulkCR;
        idx_ptr.* = i;
        return null;
    }

    fn handleBulkCr(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '\r') return .{ .Error = Error.MissingCLRF };
        i += 1;

        self.state = .BulkLF;
        idx_ptr.* = i;
        return null;
    }

    fn handleBulkLf(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '\n') return .{ .Error = Error.MissingCLRF };
        i += 1;

        self.args_count += 1;
        if (self.args_count == self.array_len) {
            return self.finishMessage(buf, i);
        }

        self.state = .ExpectBulkStart;
        idx_ptr.* = i;
        return null;
    }

    fn finishMessage(self: *StreamParser, buf: []const u8, consumed: usize) ParseResult {
        const count = self.args_count;
        var parts: [max_parts]?[]const u8 = .{null} ** max_parts;
        var idx: usize = 0;
        while (idx < count) : (idx += 1) {
            const entry = self.args_idx[idx].?;
            parts[idx] = buf[entry.start .. entry.start + entry.len];
        }

        self.cursor = consumed;
        self.state = .ExpectArrayStart;
        self.array_len = 0;
        self.bulk_remaining = 0;
        self.clearArgState();
        return .{ .Done = .{ .consumed = consumed, .parts = parts } };
    }

    pub fn init() StreamParser {
        return .{};
    }

    pub fn reset(self: *StreamParser) void {
        self.* = .{};
    }

    inline fn needMore(self: *StreamParser, idx: usize) ParseResult {
        self.cursor = idx;
        return .NeedMore;
    }

    inline fn startNumber(self: *StreamParser) void {
        self.num_acc = 0;
        self.num_had_digit = false;
    }

    inline fn clearArgState(self: *StreamParser) void {
        self.args_idx = .{null} ** max_parts;
        self.args_count = 0;
    }
};
