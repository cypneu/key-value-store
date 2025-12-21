const std = @import("std");
const types = @import("types.zig");
const constants = @import("constants.zig");
const StringStore = @import("../string.zig").StringStore;
const ListStore = @import("../list.zig").ListStore;
const StreamStore = @import("../stream.zig").StreamStore;

pub const SnapshotPipe = struct {
    read_fd: std.posix.fd_t,
    child_pid: std.posix.pid_t,
};

pub fn forkSnapshotPipe(string_store: *StringStore, list_store: *ListStore, stream_store: *StreamStore) !SnapshotPipe {
    const fds = try std.posix.pipe();
    const read_fd = fds[0];
    const write_fd = fds[1];

    const pid = std.posix.fork() catch |err| {
        std.posix.close(read_fd);
        std.posix.close(write_fd);
        return err;
    };

    if (pid == 0) {
        std.posix.close(read_fd);
        childProcessLogic(write_fd, string_store, list_store, stream_store);
        std.posix.exit(0);
    }

    std.posix.close(write_fd);
    return .{ .read_fd = read_fd, .child_pid = pid };
}

pub fn forkSnapshotPipeNonBlocking(string_store: *StringStore, list_store: *ListStore, stream_store: *StreamStore) !SnapshotPipe {
    const pipe = try forkSnapshotPipe(string_store, list_store, stream_store);
    const flags = try std.posix.fcntl(pipe.read_fd, std.posix.F.GETFL, 0);
    const nonblock: u32 = @bitCast(std.posix.O{ .NONBLOCK = true });
    _ = try std.posix.fcntl(pipe.read_fd, std.posix.F.SETFL, flags | nonblock);
    return pipe;
}

fn childProcessLogic(fd: std.posix.fd_t, string_store: *StringStore, list_store: *ListStore, stream_store: *StreamStore) void {
    const file: std.fs.File = .{ .handle = fd };
    const child_alloc = std.heap.page_allocator;
    writeSnapshot(file.writer(), child_alloc, string_store, list_store, stream_store) catch std.posix.exit(1);
    file.close();
}

pub fn generateSnapshot(allocator: std.mem.Allocator, string_store: *StringStore, list_store: *ListStore, stream_store: *StreamStore) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    try writeSnapshot(buf.writer(), allocator, string_store, list_store, stream_store);
    return buf.toOwnedSlice();
}

pub fn writeSnapshot(writer: anytype, allocator: std.mem.Allocator, string_store: *StringStore, list_store: *ListStore, stream_store: *StreamStore) !void {
    var rdb_writer = RdbWriter(@TypeOf(writer)).init(writer);
    try rdb_writer.writeHeader();
    try rdb_writer.writeStrings(allocator, string_store);
    try rdb_writer.writeLists(allocator, list_store);
    try rdb_writer.writeStreams(allocator, stream_store);
    try rdb_writer.writeEOF();
}

fn RdbWriter(comptime WriterType: type) type {
    return struct {
        writer: WriterType,

        const Self = @This();

        const OP_EOF = 0xFF;
        const OP_SELECTDB = 0xFE;
        const OP_EXPIRETIME_MS = 0xFC;

        const TYPE_STRING = 0;
        const TYPE_LIST_QUICKLIST = 18;
        const TYPE_STREAM_LISTPACKS = 21;

        fn init(w: WriterType) Self {
            return .{ .writer = w };
        }

        fn writeHeader(self: Self) !void {
            try self.writer.writeAll(constants.RDB_VERSION);
            try self.writer.writeByte(OP_SELECTDB);
            try self.writeLength(constants.DEFAULT_DB_INDEX);
        }

        fn writeStrings(self: Self, allocator: std.mem.Allocator, store: anytype) !void {
            const keys = try store.keys(allocator);
            defer allocator.free(keys);

            for (keys) |key| {
                try self.writeStringEntry(store, key);
            }
        }

        fn writeStringEntry(self: Self, store: anytype, key: []const u8) !void {
            const entry = store.getWithExpiration(key) orelse return;

            if (entry.expiration_us) |exp_us| {
                try self.writer.writeByte(OP_EXPIRETIME_MS);
                const exp_ms: u64 = @intCast(@divTrunc(exp_us, @as(i64, std.time.us_per_ms)));
                var buf: [8]u8 = undefined;
                std.mem.writeInt(u64, &buf, exp_ms, .little);
                try self.writer.writeAll(&buf);
            }

            try self.writer.writeByte(TYPE_STRING);
            try self.writeString(key);
            try self.writeString(entry.data);
        }

        fn writeLists(self: Self, allocator: std.mem.Allocator, store: anytype) !void {
            const keys = try store.keys(allocator);
            defer allocator.free(keys);

            for (keys) |key| {
                try self.writeListEntry(allocator, store, key);
            }
        }

        fn writeListEntry(self: Self, allocator: std.mem.Allocator, store: anytype, key: []const u8) !void {
            const length = store.length_by_key(key);
            if (length == 0) return;

            try self.writer.writeByte(TYPE_LIST_QUICKLIST);
            try self.writeString(key);
            try self.writeLength(1);

            const view = store.lrange(key, 0, @as(i64, @intCast(length)) - 1);
            const lp_bytes = try self.buildListListpack(allocator, view);
            defer allocator.free(lp_bytes);

            try self.writeLength(2);
            try self.writeString(lp_bytes);
        }

        fn buildListListpack(self: Self, allocator: std.mem.Allocator, view: anytype) ![]u8 {
            _ = self;
            var lp = try ListpackBuilder.init(allocator);
            defer lp.deinit();
            for (view.first) |item| try lp.appendString(item);
            for (view.second) |item| try lp.appendString(item);
            return lp.finish();
        }

        fn writeStreams(self: Self, allocator: std.mem.Allocator, store: anytype) !void {
            const keys = try store.keys(allocator);
            defer allocator.free(keys);

            for (keys) |key| {
                try self.writeStreamEntry(allocator, store, key);
            }
        }

        fn writeStreamEntry(self: Self, allocator: std.mem.Allocator, store: anytype, key: []const u8) !void {
            const entries = store.xrange(allocator, key, "-", "+") catch return;
            defer allocator.free(entries);
            if (entries.len == 0) return;

            try self.writer.writeByte(TYPE_STREAM_LISTPACKS);
            try self.writeString(key);
            try self.writeLength(1);

            const base_id = entries[0].*.id;
            var master_id: [16]u8 = undefined;
            std.mem.writeInt(u64, master_id[0..8], base_id.milliseconds, .big);
            std.mem.writeInt(u64, master_id[8..16], base_id.sequence, .big);
            try self.writeString(&master_id);

            const lp_bytes = try self.buildStreamListpack(allocator, base_id, entries);
            defer allocator.free(lp_bytes);
            try self.writeString(lp_bytes);

            const first = entries[0].*.id;
            const last = entries[entries.len - 1].*.id;

            try self.writeLength(entries.len);
            try self.writeLength(last.milliseconds);
            try self.writeLength(last.sequence);
            try self.writeLength(first.milliseconds);
            try self.writeLength(first.sequence);
            try self.writeLength(0);
            try self.writeLength(0);
            try self.writeLength(entries.len);
            try self.writeLength(0);
        }

        fn buildStreamListpack(self: Self, allocator: std.mem.Allocator, base_id: anytype, entries: anytype) ![]u8 {
            _ = self;
            var lp = try ListpackBuilder.init(allocator);
            defer lp.deinit();

            try lp.appendInt(@intCast(entries.len));
            try lp.appendInt(0);
            try lp.appendInt(0);
            try lp.appendInt(0);

            for (entries) |entry_ptr| {
                const entry = entry_ptr.*;
                try lp.appendInt(0);
                try lp.appendInt(@intCast(entry.id.milliseconds - base_id.milliseconds));
                try lp.appendInt(@intCast(entry.id.sequence - base_id.sequence));
                try lp.appendInt(@intCast(entry.fields.len));
                for (entry.fields) |field| {
                    try lp.appendString(field.name);
                    try lp.appendString(field.value);
                }
                try lp.appendInt(@intCast(entry.fields.len * 2));
            }
            return lp.finish();
        }

        fn writeEOF(self: Self) !void {
            try self.writer.writeByte(OP_EOF);
            const checksum = [_]u8{0} ** 8;
            try self.writer.writeAll(&checksum);
        }

        fn writeLength(self: Self, value: u64) !void {
            const LEN_6BIT = 0x00;
            const LEN_14BIT = 0x40;
            const LEN_32BIT = 0x80;
            const LEN_64BIT = 0x81;

            if (value < 64) {
                try self.writer.writeByte(LEN_6BIT | @as(u8, @intCast(value)));
            } else if (value < 16384) {
                const high = LEN_14BIT | @as(u8, @intCast(value >> 8));
                const low = @as(u8, @intCast(value & 0xFF));
                try self.writer.writeByte(high);
                try self.writer.writeByte(low);
            } else if (value <= std.math.maxInt(u32)) {
                try self.writer.writeByte(LEN_32BIT);
                var buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &buf, @intCast(value), .big);
                try self.writer.writeAll(&buf);
            } else {
                try self.writer.writeByte(LEN_64BIT);
                var buf: [8]u8 = undefined;
                std.mem.writeInt(u64, &buf, value, .big);
                try self.writer.writeAll(&buf);
            }
        }

        fn writeString(self: Self, data: []const u8) !void {
            try self.writeLength(data.len);
            try self.writer.writeAll(data);
        }
    };
}

pub const ListpackBuilder = struct {
    buf: std.ArrayList(u8),
    count: usize,

    const INT_16BIT = 0xF1;
    const INT_32BIT = 0xF3;
    const INT_64BIT = 0xF4;
    const STR_32BIT = 0xF0;
    const EOF = 0xFF;

    pub fn init(allocator: std.mem.Allocator) !ListpackBuilder {
        var buf = std.ArrayList(u8).init(allocator);
        try buf.appendNTimes(0, 6);
        return .{ .buf = buf, .count = 0 };
    }

    pub fn deinit(self: *ListpackBuilder) void {
        self.buf.deinit();
    }

    pub fn appendInt(self: *ListpackBuilder, value: i64) !void {
        const start = self.buf.items.len;

        if (value >= 0 and value < 128) {
            try self.buf.append(@intCast(value));
        } else if (value >= std.math.minInt(i16) and value <= std.math.maxInt(i16)) {
            try self.buf.append(INT_16BIT);
            var buf: [2]u8 = undefined;
            std.mem.writeInt(i16, &buf, @intCast(value), .little);
            try self.buf.appendSlice(&buf);
        } else if (value >= std.math.minInt(i32) and value <= std.math.maxInt(i32)) {
            try self.buf.append(INT_32BIT);
            var buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &buf, @intCast(value), .little);
            try self.buf.appendSlice(&buf);
        } else {
            try self.buf.append(INT_64BIT);
            var buf: [8]u8 = undefined;
            std.mem.writeInt(i64, &buf, value, .little);
            try self.buf.appendSlice(&buf);
        }

        const len = self.buf.items.len - start;
        try self.appendBacklen(len);
        self.count += 1;
    }

    pub fn appendString(self: *ListpackBuilder, data: []const u8) !void {
        const start = self.buf.items.len;

        if (data.len < 64) {
            try self.buf.append(0x80 | @as(u8, @intCast(data.len)));
            try self.buf.appendSlice(data);
        } else if (data.len < 4096) {
            const high = 0xE0 | @as(u8, @intCast((data.len >> 8) & 0x0F));
            const low = @as(u8, @intCast(data.len & 0xFF));
            try self.buf.append(high);
            try self.buf.append(low);
            try self.buf.appendSlice(data);
        } else {
            try self.buf.append(STR_32BIT);
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, @intCast(data.len), .little);
            try self.buf.appendSlice(&buf);
            try self.buf.appendSlice(data);
        }

        const len = self.buf.items.len - start;
        try self.appendBacklen(len);
        self.count += 1;
    }

    pub fn finish(self: *ListpackBuilder) ![]u8 {
        try self.buf.append(EOF);

        const total = @as(u32, @intCast(self.buf.items.len));
        std.mem.writeInt(u32, self.buf.items[0..4], total, .little);
        std.mem.writeInt(u16, self.buf.items[4..6], @intCast(self.count), .little);

        return self.buf.toOwnedSlice();
    }

    fn appendBacklen(self: *ListpackBuilder, len: usize) !void {
        if (len <= 127) {
            try self.buf.append(@intCast(len));
        } else if (len < 16383) {
            try self.buf.append(@intCast(len >> 7));
            try self.buf.append(@intCast((len & 127) | 128));
        } else if (len < 2097151) {
            try self.buf.append(@intCast(len >> 14));
            try self.buf.append(@intCast(((len >> 7) & 127) | 128));
            try self.buf.append(@intCast((len & 127) | 128));
        } else if (len < 268435455) {
            try self.buf.append(@intCast(len >> 21));
            try self.buf.append(@intCast(((len >> 14) & 127) | 128));
            try self.buf.append(@intCast(((len >> 7) & 127) | 128));
            try self.buf.append(@intCast((len & 127) | 128));
        } else {
            try self.buf.append(@intCast(len >> 28));
            try self.buf.append(@intCast(((len >> 21) & 127) | 128));
            try self.buf.append(@intCast(((len >> 14) & 127) | 128));
            try self.buf.append(@intCast(((len >> 7) & 127) | 128));
            try self.buf.append(@intCast((len & 127) | 128));
        }
    }
};

test "ListpackBuilder int" {
    var lp = try ListpackBuilder.init(std.testing.allocator);
    defer lp.deinit();
    try lp.appendInt(100);
    try lp.appendInt(200);
    const bytes = try lp.finish();
    defer std.testing.allocator.free(bytes);

    try std.testing.expect(bytes.len > 6);
    try std.testing.expectEqual(@as(u8, 100), bytes[6]);
}

test "ListpackBuilder string" {
    var lp = try ListpackBuilder.init(std.testing.allocator);
    defer lp.deinit();
    try lp.appendString("hello");
    const bytes = try lp.finish();
    defer std.testing.allocator.free(bytes);

    const expected_header = 0x80 | 5;
    try std.testing.expectEqual(@as(u8, expected_header), bytes[6]);
    try std.testing.expectEqualStrings("hello", bytes[7..12]);
}
