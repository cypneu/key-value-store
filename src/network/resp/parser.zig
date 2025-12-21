const std = @import("std");

pub const StreamParser = struct {
    pub const Error = error{
        NotArray,
        MissingArraySize,
        MissingCLRF,
        Overflow,
        OutOfMemory,
    };

    pub const MessageKind = enum { Array, SimpleString, TopLevelBulk };

    pub const ParseResult = union(enum) {
        NeedMore,
        Done: struct { consumed: usize, parts: []const []const u8, kind: MessageKind },
        Error: Error,
    };

    const State = enum {
        ExpectStart,
        // Array states
        ArrayLenDigits,
        ExpectLFArrayLen,
        ExpectArgOrDone,
        ExpectBulkStart,
        // Bulk states (shared)
        BulkLenDigits,
        ExpectLFBulkLen,
        BulkData,
        BulkCR,
        BulkLF,
        // Simple String states
        SimpleString,
        SimpleStringCR,
        SimpleStringLF,
    };

    allocator: std.mem.Allocator,
    args_idx: std.ArrayList(PartIndex),
    parts: std.ArrayList([]const u8),

    state: State = .ExpectStart,
    mode: MessageKind = .Array,
    cursor: usize = 0,

    array_len: usize = 0,

    num_acc: u64 = 0,
    num_had_digit: bool = false,

    bulk_remaining: usize = 0,
    simple_string_start: usize = 0,

    const PartIndex = struct { start: usize, len: usize };

    pub fn parse(self: *StreamParser, buf: []const u8) ParseResult {
        var i: usize = self.cursor;

        while (true) {
            const outcome = switch (self.state) {
                .ExpectStart => self.handleExpectStart(buf, &i),
                .ArrayLenDigits => self.handleArrayLenDigits(buf, &i),
                .ExpectLFArrayLen => self.handleExpectLfArrayLen(buf, &i),
                .ExpectArgOrDone => self.handleExpectArgOrDone(buf, &i),
                .ExpectBulkStart => self.handleExpectBulkStart(buf, &i),
                .BulkLenDigits => self.handleBulkLenDigits(buf, &i),
                .ExpectLFBulkLen => self.handleExpectLfBulkLen(buf, &i),
                .BulkData => self.handleBulkData(buf, &i),
                .BulkCR => self.handleBulkCr(buf, &i),
                .BulkLF => self.handleBulkLf(buf, &i),
                .SimpleString => self.handleSimpleString(buf, &i),
                .SimpleStringCR => self.handleSimpleStringCr(buf, &i),
                .SimpleStringLF => self.handleSimpleStringLf(buf, &i),
            };

            if (outcome) |result| return result;
        }
    }

    fn handleExpectStart(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        const prefix = buf[i];
        i += 1;

        switch (prefix) {
            '*' => {
                self.mode = .Array;
                self.startNumber();
                self.state = .ArrayLenDigits;
                idx_ptr.* = i;
                return null;
            },
            '$' => {
                self.mode = .TopLevelBulk;
                self.startNumber();
                self.state = .BulkLenDigits;
                idx_ptr.* = i;
                return null;
            },
            '+' => {
                self.mode = .SimpleString;
                self.simple_string_start = i;
                self.state = .SimpleString;
                idx_ptr.* = i;
                return null;
            },
            else => return .{ .Error = Error.NotArray },
        }
    }

    fn handleSimpleString(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;

        while (i < buf.len) {
            if (buf[i] == '\r') {
                self.clearArgState();
                self.args_idx.append(PartIndex{ .start = self.simple_string_start, .len = i - self.simple_string_start }) catch return .{ .Error = Error.OutOfMemory };

                i += 1;
                self.state = .SimpleStringLF;
                idx_ptr.* = i;
                return null;
            }
            i += 1;
        }

        return self.needMore(i);
    }

    fn handleSimpleStringCr(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '\r') return .{ .Error = Error.MissingCLRF };
        i += 1;

        self.state = .SimpleStringLF;
        idx_ptr.* = i;
        return null;
    }

    fn handleSimpleStringLf(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;
        if (i >= buf.len) return self.needMore(i);

        if (buf[i] != '\n') return .{ .Error = Error.MissingCLRF };
        i += 1;

        return self.finishMessage(buf, i);
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

        if (self.num_acc > @as(u64, std.math.maxInt(usize))) return .{ .Error = Error.Overflow };
        self.array_len = @intCast(self.num_acc);
        self.clearArgState();
        self.args_idx.ensureTotalCapacity(self.array_len) catch return .{ .Error = Error.OutOfMemory };
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

        if (self.num_acc > @as(u64, std.math.maxInt(usize))) return .{ .Error = Error.Overflow };
        self.bulk_remaining = @intCast(self.num_acc);
        self.state = .BulkData;
        idx_ptr.* = i;
        return null;
    }

    fn handleBulkData(self: *StreamParser, buf: []const u8, idx_ptr: *usize) ?ParseResult {
        var i = idx_ptr.*;

        if (self.bulk_remaining == 0) {
            const start = i;
            self.args_idx.append(PartIndex{ .start = start, .len = 0 }) catch return .{ .Error = Error.OutOfMemory };
            self.bulk_remaining = 0;
            self.state = .BulkCR;
            idx_ptr.* = i;
            return null;
        }

        const available = buf.len - i;
        if (available < self.bulk_remaining) return self.needMore(i);

        const start = i;
        const end = i + self.bulk_remaining;
        self.args_idx.append(PartIndex{ .start = start, .len = end - start }) catch return .{ .Error = Error.OutOfMemory };
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

        if (self.mode == .TopLevelBulk) {
            return self.finishMessage(buf, i);
        } else {
            if (self.args_idx.items.len == self.array_len) {
                return self.finishMessage(buf, i);
            }
            self.state = .ExpectBulkStart;
        }

        idx_ptr.* = i;
        return null;
    }

    fn finishMessage(self: *StreamParser, buf: []const u8, consumed: usize) ParseResult {
        const kind = self.mode;
        const count = self.args_idx.items.len;

        self.parts.clearRetainingCapacity();
        self.parts.ensureTotalCapacity(count) catch return .{ .Error = Error.OutOfMemory };
        for (self.args_idx.items) |entry| {
            self.parts.appendAssumeCapacity(buf[entry.start .. entry.start + entry.len]);
        }

        self.cursor = consumed;
        self.state = .ExpectStart;
        self.mode = .Array;
        self.array_len = 0;
        self.bulk_remaining = 0;
        self.clearArgState();
        return .{ .Done = .{ .consumed = consumed, .parts = self.parts.items, .kind = kind } };
    }

    pub fn init(allocator: std.mem.Allocator) StreamParser {
        return .{
            .allocator = allocator,
            .args_idx = std.ArrayList(PartIndex).init(allocator),
            .parts = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn reset(self: *StreamParser) void {
        self.state = .ExpectStart;
        self.mode = .Array;
        self.cursor = 0;
        self.array_len = 0;
        self.args_idx.clearRetainingCapacity();
        self.parts.clearRetainingCapacity();
        self.num_acc = 0;
        self.num_had_digit = false;
        self.bulk_remaining = 0;
        self.simple_string_start = 0;
    }

    pub fn deinit(self: *StreamParser) void {
        self.args_idx.deinit();
        self.parts.deinit();
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
        self.args_idx.clearRetainingCapacity();
    }
};
