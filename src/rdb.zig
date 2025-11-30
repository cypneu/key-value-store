const std = @import("std");
const db = @import("data_structures/mod.zig");
const EntryId = std.meta.FieldType(db.StreamEntry, .id);

const DEFAULT_DB_INDEX: u8 = 0;
const RDB_VERSION = "REDIS0012";

pub const RdbError = error{
    InvalidHeader,
    InvalidChecksum,
    UnsupportedEncoding,
    UnexpectedEOF,
    InvalidLengthEncoding,
    InvalidListpackEncoding,
    ExpectedInteger,
};

pub fn loadRDBFromBytes(allocator: std.mem.Allocator, bytes: []const u8, handler: anytype) !void {
    var stream = std.io.fixedBufferStream(bytes);
    var loader = RdbLoader(@TypeOf(stream.reader()), @TypeOf(handler)).init(allocator, stream.reader(), handler);
    try loader.load();
}

pub fn generateSnapshot(allocator: std.mem.Allocator, handler: anytype) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try writeSnapshot(buf.writer(), allocator, handler);

    return try buf.toOwnedSlice();
}

pub fn writeSnapshot(writer: anytype, allocator: std.mem.Allocator, handler: anytype) !void {
    try writer.writeAll(RDB_VERSION);

    // Select default DB
    try writer.writeByte(0xFE);
    try writeLength(writer, DEFAULT_DB_INDEX);

    // Strings (with expirations)
    const string_keys = try handler.string_store.keys(allocator);
    defer allocator.free(string_keys);

    for (string_keys) |key| {
        const entry = handler.string_store.getWithExpiration(key) orelse continue;
        if (entry.expiration_us) |exp_us| {
            try writer.writeByte(0xFC);
            const exp_ms: u64 = @intCast(@divTrunc(exp_us, @as(i64, std.time.us_per_ms)));
            var exp_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, exp_buf[0..], exp_ms, .little);
            try writer.writeAll(&exp_buf);
        }

        try writer.writeByte(RDB_TYPE_STRING);
        try writeString(writer, key);
        try writeString(writer, entry.data);
    }

    // Lists
    const list_keys = try handler.list_store.keys(allocator);
    defer allocator.free(list_keys);

    for (list_keys) |key| {
        const length = handler.list_store.length_by_key(key);
        if (length == 0) continue;

        try writer.writeByte(RDB_TYPE_LIST);
        try writeString(writer, key);
        try writeLength(writer, length);

        const view = handler.list_store.lrange(key, 0, @as(i64, @intCast(length)) - 1);
        for (view.first) |item| try writeString(writer, item);
        for (view.second) |item| try writeString(writer, item);
    }

    // Streams
    const stream_keys = try handler.stream_store.keys(allocator);
    defer allocator.free(stream_keys);

    for (stream_keys) |key| {
        const entries = handler.stream_store.xrange(allocator, key, "-", "+") catch |err| switch (err) {
            error.InvalidRangeId => continue,
            else => return err,
        };
        defer allocator.free(entries);

        if (entries.len == 0) continue;

        try writer.writeByte(RDB_TYPE_STREAM_LISTPACKS);
        try writeString(writer, key);

        // One listpack per stream
        try writeLength(writer, 1);

        const base_id = entries[0].*.id;
        var master_id: [16]u8 = undefined;
        std.mem.writeInt(u64, master_id[0..8], base_id.milliseconds, .big);
        std.mem.writeInt(u64, master_id[8..16], base_id.sequence, .big);
        try writeString(writer, &master_id);

        const lp_bytes = try encodeStreamListpack(allocator, base_id, entries);
        defer allocator.free(lp_bytes);
        try writeString(writer, lp_bytes);

        const last_id = entries[entries.len - 1].*.id;
        try writeLength(writer, entries.len);
        try writeLength(writer, last_id.milliseconds);
        try writeLength(writer, last_id.sequence);

        // Consumer groups: none
        try writeLength(writer, 0);
    }

    // EOF
    try writer.writeByte(0xFF);
    var checksum_buf = [_]u8{0} ** 8;
    try writer.writeAll(&checksum_buf); // checksum placeholder
}

pub const SnapshotPipe = struct {
    read_fd: std.posix.fd_t,
    child_pid: std.posix.pid_t,
};

pub fn forkSnapshotPipe(handler: anytype) !SnapshotPipe {
    const fds = try std.posix.pipe();
    const read_fd = fds[0];
    const write_fd = fds[1];

    const pid = std.posix.fork() catch |err| {
        std.posix.close(read_fd);
        std.posix.close(write_fd);
        return err;
    };

    if (pid == 0) {
        // Child: build snapshot and stream to pipe, then exit quickly.
        std.posix.close(read_fd);
        const child_alloc = std.heap.page_allocator;

        const snapshot = generateSnapshot(child_alloc, handler) catch std.posix.exit(1);

        var len_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, len_buf[0..], @intCast(snapshot.len), .little);

        var written: usize = 0;
        // Write length prefix
        while (written < len_buf.len) {
            const nwritten = std.posix.write(write_fd, len_buf[written..]) catch |err| {
                if (err == error.Interrupted) continue;
                std.posix.exit(1);
            };
            if (nwritten == 0) break;
            written += nwritten;
        }

        written = 0;
        while (written < snapshot.len) {
            const nwritten = std.posix.write(write_fd, snapshot[written..]) catch |err| {
                if (err == error.Interrupted) continue;
                std.posix.exit(1);
            };
            if (nwritten == 0) break;
            written += nwritten;
        }

        std.posix.close(write_fd);
        std.posix.exit(0);
    }

    // Parent
    std.posix.close(write_fd);

    return .{ .read_fd = read_fd, .child_pid = pid };
}

fn writeLength(writer: anytype, value: u64) !void {
    if (value < 0x40) {
        try writer.writeByte(@intCast(value));
    } else if (value < 0x4000) {
        const b0: u8 = 0x40 | @as(u8, @intCast(value >> 8));
        const b1: u8 = @as(u8, @intCast(value & 0xFF));
        try writer.writeByte(b0);
        try writer.writeByte(b1);
    } else if (value <= std.math.maxInt(u32)) {
        try writer.writeByte(0x80);
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, buf[0..], @intCast(value), .big);
        try writer.writeAll(&buf);
    } else {
        try writer.writeByte(0x81);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, buf[0..], value, .big);
        try writer.writeAll(&buf);
    }
}

fn writeString(writer: anytype, data: []const u8) !void {
    try writeLength(writer, data.len);
    try writer.writeAll(data);
}

fn encodeStreamListpack(allocator: std.mem.Allocator, base_id: EntryId, entries: []*const db.StreamEntry) ![]u8 {
    var lp = try ListpackBuilder.init(allocator);
    errdefer lp.deinit();

    try lp.appendInt(@intCast(entries.len)); // count
    try lp.appendInt(0); // deleted
    try lp.appendInt(0); // master fields
    try lp.appendInt(0); // terminator

    for (entries) |entry_ptr| {
        const entry = entry_ptr.*;
        try lp.appendInt(0); // flags
        try lp.appendInt(@intCast(entry.id.milliseconds - base_id.milliseconds));
        try lp.appendInt(@intCast(entry.id.sequence - base_id.sequence));

        try lp.appendInt(@intCast(entry.fields.len));
        for (entry.fields) |field| {
            try lp.appendString(field.name);
            try lp.appendString(field.value);
        }

        try lp.appendInt(@intCast(entry.fields.len * 2));
    }

    return try lp.finish();
}

const ListpackBuilder = struct {
    buf: std.ArrayList(u8),
    count: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !ListpackBuilder {
        var buf = std.ArrayList(u8).init(allocator);
        var i: usize = 0;
        while (i < 6) : (i += 1) {
            try buf.append(0);
        }
        return .{ .buf = buf, .count = 0 };
    }

    pub fn deinit(self: *ListpackBuilder) void {
        self.buf.deinit();
    }

    fn appendBacklen(self: *ListpackBuilder, element_len: usize) !void {
        if (element_len <= 127) {
            try self.buf.append(@intCast(element_len));
        } else if (element_len < 16383) {
            try self.buf.append(@intCast(element_len >> 7));
            try self.buf.append(@intCast((element_len & 127) | 128));
        } else if (element_len < 2097151) {
            try self.buf.append(@intCast(element_len >> 14));
            try self.buf.append(@intCast(((element_len >> 7) & 127) | 128));
            try self.buf.append(@intCast((element_len & 127) | 128));
        } else if (element_len < 268435455) {
            try self.buf.append(@intCast(element_len >> 21));
            try self.buf.append(@intCast(((element_len >> 14) & 127) | 128));
            try self.buf.append(@intCast(((element_len >> 7) & 127) | 128));
            try self.buf.append(@intCast((element_len & 127) | 128));
        } else {
            try self.buf.append(@intCast(element_len >> 28));
            try self.buf.append(@intCast(((element_len >> 21) & 127) | 128));
            try self.buf.append(@intCast(((element_len >> 14) & 127) | 128));
            try self.buf.append(@intCast(((element_len >> 7) & 127) | 128));
            try self.buf.append(@intCast((element_len & 127) | 128));
        }
    }

    pub fn appendInt(self: *ListpackBuilder, value: i64) !void {
        const start = self.buf.items.len;

        if (value >= 0 and value < 128) {
            try self.buf.append(@intCast(value));
        } else if (value >= std.math.minInt(i16) and value <= std.math.maxInt(i16)) {
            try self.buf.append(0xF1);
            var tmp: [2]u8 = undefined;
            std.mem.writeInt(i16, tmp[0..], @intCast(value), .little);
            try self.buf.appendSlice(&tmp);
        } else if (value >= std.math.minInt(i32) and value <= std.math.maxInt(i32)) {
            try self.buf.append(0xF3);
            var tmp: [4]u8 = undefined;
            std.mem.writeInt(i32, tmp[0..], @intCast(value), .little);
            try self.buf.appendSlice(&tmp);
        } else {
            try self.buf.append(0xF4);
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, tmp[0..], value, .little);
            try self.buf.appendSlice(&tmp);
        }

        const element_len = self.buf.items.len - start;
        try self.appendBacklen(element_len);
        self.count += 1;
    }

    pub fn appendString(self: *ListpackBuilder, data: []const u8) !void {
        const start = self.buf.items.len;
        if (data.len < 64) {
            const prefix: u8 = 0x80 | @as(u8, @intCast(data.len));
            try self.buf.append(prefix);
            try self.buf.appendSlice(data);
        } else if (data.len < 4096) {
            const high: u8 = 0xE0 | @as(u8, @intCast((data.len >> 8) & 0x0F));
            const low: u8 = @as(u8, @intCast(data.len & 0xFF));
            try self.buf.append(high);
            try self.buf.append(low);
            try self.buf.appendSlice(data);
        } else {
            try self.buf.append(0xF0);
            var tmp: [4]u8 = undefined;
            std.mem.writeInt(u32, tmp[0..], @intCast(data.len), .little);
            try self.buf.appendSlice(&tmp);
            try self.buf.appendSlice(data);
        }

        const element_len = self.buf.items.len - start;
        try self.appendBacklen(element_len);
        self.count += 1;
    }

    pub fn finish(self: *ListpackBuilder) ![]u8 {
        try self.buf.append(0xFF);

        if (self.buf.items.len < 6) return error.InvalidLengthEncoding;

        const total_bytes: u32 = @intCast(self.buf.items.len);
        std.mem.writeInt(u32, self.buf.items[0..4], total_bytes, .little);
        std.mem.writeInt(u16, self.buf.items[4..6], @intCast(self.count), .little);

        return try self.buf.toOwnedSlice();
    }
};

// RDB Value Types
const RDB_TYPE_STRING = 0;
const RDB_TYPE_LIST = 1;
const RDB_TYPE_LIST_QUICKLIST = 14;
const RDB_TYPE_STREAM_LISTPACKS = 15;
const RDB_TYPE_LIST_QUICKLIST_2 = 18;
const RDB_TYPE_STREAM_LISTPACKS_2 = 19;
const RDB_TYPE_STREAM_LISTPACKS_3 = 21;

pub fn loadRDB(allocator: std.mem.Allocator, dir: []const u8, filename: []const u8, handler: anytype) !void {
    const path = try std.fs.path.join(allocator, &[_][]const u8{ dir, filename });
    defer allocator.free(path);

    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        if (err == error.FileNotFound) return;
        return err;
    };
    defer file.close();

    var buf_reader = std.io.bufferedReader(file.reader());
    var loader = RdbLoader(@TypeOf(buf_reader.reader()), @TypeOf(handler)).init(allocator, buf_reader.reader(), handler);
    try loader.load();
}

fn RdbLoader(comptime Reader: type, comptime Handler: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: Reader,
        handler: Handler,
        peeked_byte: ?u8 = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: Reader, handler: Handler) Self {
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
                        // std.debug.print("Skipping top-level opcode: {X}\n", .{byte});
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
            const opcode = self.readByte() catch |err| {
                if (err == error.EndOfStream) return false;
                return err;
            };

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
            const key = try self.readString();
            defer self.allocator.free(key);

            var expire_us: ?i64 = null;
            if (expire_ms) |ms| {
                expire_us = @as(i64, @intCast(ms)) * 1000;
            }

            switch (value_type) {
                RDB_TYPE_STRING => {
                    const val = try self.readString();
                    defer self.allocator.free(val);
                    try self.handler.string_store.set(key, val, expire_us);
                },
                RDB_TYPE_LIST, RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2 => {
                    try self.loadList(key, value_type);
                },
                RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2, RDB_TYPE_STREAM_LISTPACKS_3 => {
                    try self.loadStream(key, value_type);
                },
                else => return RdbError.UnsupportedEncoding,
            }
            return true;
        }

        fn loadList(self: *Self, key: []const u8, value_type: u8) !void {
            const len_info = try self.readLength();
            var i: u64 = 0;
            while (i < len_info.len) : (i += 1) {
                if (value_type == RDB_TYPE_LIST_QUICKLIST_2) {
                    const container = try self.readLength(); // Container ID

                    // Container ID 2 indicates Listpack encoding
                    if (container.len == 2) {
                        const blob = try self.readString();
                        defer self.allocator.free(blob);

                        var lp = ListpackReader.init(blob);
                        while (!lp.isAtEnd()) {
                            const val = try lp.readStringOwned(self.allocator);
                            defer self.allocator.free(val);
                            _ = try self.handler.list_store.append(key, val);
                        }
                    } else {
                        // Skip other container types (e.g. Ziplist if ever used here)
                        const blob = try self.readString();
                        self.allocator.free(blob);
                    }
                } else if (value_type == RDB_TYPE_LIST) {
                    const element = try self.readString();
                    defer self.allocator.free(element);
                    _ = try self.handler.list_store.append(key, element);
                } else {
                    // Quicklist (Type 14) / Ziplist
                    const container = try self.readString();
                    self.allocator.free(container);
                }
            }
        }

        const StreamEntryFlag = struct {
            const None = 0;
            const Deleted = 1 << 0;
            const SameFields = 1 << 1;
        };

        fn loadStream(self: *Self, key: []const u8, value_type: u8) !void {
            // 1. Listpacks (Data)
            const listpacks_count = try self.readLength();
            var i: u64 = 0;
            while (i < listpacks_count.len) : (i += 1) {
                const master_id_str = try self.readString();
                defer self.allocator.free(master_id_str);

                var ms_base: u64 = 0;
                var seq_base: u64 = 0;

                // RDB Stream Master ID is stored as 16 raw bytes (Big Endian)
                if (master_id_str.len == 16) {
                    ms_base = std.mem.readInt(u64, master_id_str[0..8], .big);
                    seq_base = std.mem.readInt(u64, master_id_str[8..16], .big);
                } else {
                    // Legacy string format fallback (unlikely for modern RDB types 15+)
                    var it = std.mem.splitScalar(u8, master_id_str, '-');
                    if (it.next()) |m| ms_base = std.fmt.parseInt(u64, m, 10) catch 0;
                    if (it.next()) |s| seq_base = std.fmt.parseInt(u64, s, 10) catch 0;
                }

                const listpack_blob = try self.readString();
                defer self.allocator.free(listpack_blob);

                var lp = ListpackReader.init(listpack_blob);

                // --- Master Entry (Header) ---
                const count_val = try lp.readInt();
                const deleted_val = try lp.readInt();
                const num_master_fields = try lp.readInt();

                var master_fields = try std.ArrayList([]const u8).initCapacity(self.allocator, @intCast(num_master_fields));
                defer master_fields.deinit();

                var mf_i: u64 = 0;
                while (mf_i < num_master_fields) : (mf_i += 1) {
                    const field = try lp.readStringOwned(self.allocator);
                    master_fields.appendAssumeCapacity(field);
                }
                defer {
                    for (master_fields.items) |f| self.allocator.free(f);
                }

                _ = try lp.readInt(); // Zero terminator for Master Entry

                // --- Data Entries ---
                const total_entries = count_val + deleted_val;
                var ent_i: u64 = 0;
                while (ent_i < total_entries) : (ent_i += 1) {
                    const flags = try lp.readInt();
                    const ms_delta = try lp.readInt();
                    const seq_delta = try lp.readInt();

                    const entry_ms = ms_base + @as(u64, @intCast(ms_delta));
                    const entry_seq = seq_base + @as(u64, @intCast(seq_delta));

                    const is_deleted = (flags & StreamEntryFlag.Deleted) != 0;
                    const same_fields = (flags & StreamEntryFlag.SameFields) != 0;

                    var field_values = std.ArrayList(db.StreamFieldValue).init(self.allocator);
                    defer field_values.deinit();

                    if (same_fields) {
                        for (master_fields.items) |field_name| {
                            const val_str = try lp.readStringOwned(self.allocator);
                            try field_values.append(.{ .field = field_name, .value = val_str });
                        }
                    } else {
                        const num_fields = try lp.readInt();
                        var nf: u64 = 0;
                        while (nf < num_fields) : (nf += 1) {
                            const f_str = try lp.readStringOwned(self.allocator);
                            const v_str = try lp.readStringOwned(self.allocator);
                            try field_values.append(.{ .field = f_str, .value = v_str });
                        }
                    }

                    _ = try lp.readInt(); // lp-count

                    if (!is_deleted) {
                        var id_buf: [64]u8 = undefined;
                        const id_slice = try std.fmt.bufPrint(&id_buf, "{d}-{d}", .{ entry_ms, entry_seq });

                        const added_id = try self.handler.stream_store.addEntry(self.allocator, key, id_slice, field_values.items);
                        self.allocator.free(added_id);
                    }

                    for (field_values.items) |fv| {
                        if (!same_fields) self.allocator.free(fv.field);
                        self.allocator.free(fv.value);
                    }
                }
            }

            // 2. Metadata
            _ = try self.readLength(); // Total items
            _ = try self.readLength(); // Last ID ms
            _ = try self.readLength(); // Last ID seq

            // RDB Stream Types 19 and 21 (Redis 5.0+ / 7.0+) have extra metadata
            if (value_type == RDB_TYPE_STREAM_LISTPACKS_2 or value_type == RDB_TYPE_STREAM_LISTPACKS_3) {
                _ = try self.readLength(); // First ID ms
                _ = try self.readLength(); // First ID seq
                _ = try self.readLength(); // Max Deleted ID ms
                _ = try self.readLength(); // Max Deleted ID seq
                _ = try self.readLength(); // Entries Added
            }

            // 3. Consumer Groups
            const cgroups_count = try self.readLength();
            var cg_i: u64 = 0;
            while (cg_i < cgroups_count.len) : (cg_i += 1) {
                // Skip Consumer Group Name
                const cg_name = try self.readString();
                self.allocator.free(cg_name);

                _ = try self.readLength(); // Last CG ID ms
                _ = try self.readLength(); // Last CG ID seq

                // Skip Global PEL
                try self.skipStreamPEL();

                // Consumers
                const consumers_count = try self.readLength();
                var c_i: u64 = 0;
                while (c_i < consumers_count.len) : (c_i += 1) {
                    const consumer_name = try self.readString();
                    self.allocator.free(consumer_name);

                    // Seen time is 8 bytes little endian
                    _ = try self.reader.readInt(u64, .little);

                    // Skip Consumer PEL
                    try self.skipStreamPEL();
                }
            }
        }

        fn skipStreamPEL(self: *Self) !void {
            const pending_count = try self.readLength();
            var i: u64 = 0;
            while (i < pending_count.len) : (i += 1) {
                var raw_id: [16]u8 = undefined;
                try self.reader.readNoEof(&raw_id);
                _ = try self.reader.readInt(u64, .little); // delivery_time (8 bytes)
                _ = try self.readLength(); // delivery_count
            }
        }

        // --- Low Level Readers ---

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
                    const subtype = first_byte & 0x3F;
                    if (subtype == 1) { // 0x81: 64-bit length
                        const len = try self.reader.readInt(u64, .big);
                        return .{ .len = len, .is_encoded = false, .encoding_type = 0 };
                    } else { // 0x80 (32-bit) or fallback
                        const len = try self.reader.readInt(u32, .big);
                        return .{ .len = len, .is_encoded = false, .encoding_type = 0 };
                    }
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
                    3 => {
                        // LZF Compression
                        const clen_info = try self.readLength(); // Compressed length
                        const ulen_info = try self.readLength(); // Uncompressed length

                        // 1. Read the compressed data
                        const compressed = try self.allocator.alloc(u8, clen_info.len);
                        defer self.allocator.free(compressed);
                        try self.reader.readNoEof(compressed);

                        // 2. Allocate buffer for uncompressed data
                        const uncompressed = try self.allocator.alloc(u8, ulen_info.len);
                        errdefer self.allocator.free(uncompressed);

                        // 3. Decompress
                        lzfDecompress(compressed, uncompressed) catch {
                            return RdbError.UnsupportedEncoding; // Or return err
                        };

                        return uncompressed;
                    },
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

// Helper: Listpack Reader for Redis Streams
const ListpackReader = struct {
    data: []const u8,
    cursor: usize = 0,

    pub fn init(data: []const u8) ListpackReader {
        // Skip Header: Total Bytes (4 bytes) + Num Elements (2 bytes) = 6 bytes
        if (data.len < 6) return .{ .data = data, .cursor = 0 };
        return .{ .data = data, .cursor = 6 };
    }

    pub fn isAtEnd(self: *ListpackReader) bool {
        return self.cursor >= self.data.len or self.data[self.cursor] == 0xFF;
    }

    fn skipBacklen(self: *ListpackReader, element_len: usize) void {
        const bl_size: usize = if (element_len <= 127) 1 else if (element_len < 16383) 2 else if (element_len < 2097151) 3 else if (element_len < 268435455) 4 else 5;
        self.cursor += bl_size;
    }

    pub fn readInt(self: *ListpackReader) !i64 {
        const start_cursor = self.cursor;
        if (self.cursor >= self.data.len) return RdbError.UnexpectedEOF;

        const b = self.data[self.cursor];
        self.cursor += 1;

        var val: i64 = 0;

        if ((b & 0x80) == 0) { // 0xxxxxxx: 7-bit uint
            val = @intCast(b);
        } else if ((b & 0xC0) == 0x80) { // 10xxxxxx: 6-bit string length
            const len = b & 0x3F;
            const str_val = try self.readBytes(len);
            val = std.fmt.parseInt(i64, str_val, 10) catch 0;
        } else if ((b & 0xE0) == 0xC0) { // 110xxxxx: 13-bit int
            if (self.cursor >= self.data.len) return RdbError.UnexpectedEOF;
            const next = self.data[self.cursor];
            self.cursor += 1;

            const uval = (@as(u16, b & 0x1F) << 8) | @as(u16, next);
            const shifted = @as(i16, @bitCast(uval << 3));
            val = @as(i64, shifted >> 3);
        } else if ((b & 0xF0) == 0xE0) { // 1110xxxx: 12-bit string len
            const high = b & 0x0F;
            if (self.cursor >= self.data.len) return RdbError.UnexpectedEOF;
            const low = self.data[self.cursor];
            self.cursor += 1;
            const len = (@as(usize, high) << 8) | @as(usize, low);
            const s = try self.readBytes(len);
            val = std.fmt.parseInt(i64, s, 10) catch 0;
        } else {
            switch (b) {
                0xF1 => { // 16-bit int
                    if (self.cursor + 2 > self.data.len) return RdbError.UnexpectedEOF;
                    val = std.mem.readInt(i16, self.data[self.cursor..][0..2], .little);
                    self.cursor += 2;
                },
                0xF2 => { // 24-bit int
                    if (self.cursor + 3 > self.data.len) return RdbError.UnexpectedEOF;
                    const ptr = self.data[self.cursor..];
                    const v32 = @as(i32, ptr[0]) | (@as(i32, ptr[1]) << 8) | (@as(i32, @intCast(ptr[2])) << 16);
                    self.cursor += 3;
                    val = (v32 << 8) >> 8;
                },
                0xF3 => { // 32-bit int
                    if (self.cursor + 4 > self.data.len) return RdbError.UnexpectedEOF;
                    val = std.mem.readInt(i32, self.data[self.cursor..][0..4], .little);
                    self.cursor += 4;
                },
                0xF4 => { // 64-bit int
                    if (self.cursor + 8 > self.data.len) return RdbError.UnexpectedEOF;
                    val = std.mem.readInt(i64, self.data[self.cursor..][0..8], .little);
                    self.cursor += 8;
                },
                0xF0 => { // 32-bit string len
                    if (self.cursor + 4 > self.data.len) return RdbError.UnexpectedEOF;
                    const l = std.mem.readInt(u32, self.data[self.cursor..][0..4], .little);
                    self.cursor += 4;
                    const s = try self.readBytes(l);
                    val = std.fmt.parseInt(i64, s, 10) catch 0;
                },
                else => return RdbError.InvalidListpackEncoding,
            }
        }

        const element_len = self.cursor - start_cursor;
        self.skipBacklen(element_len);
        return val;
    }

    pub fn readStringOwned(self: *ListpackReader, allocator: std.mem.Allocator) ![]const u8 {
        const start_cursor = self.cursor;
        if (self.cursor >= self.data.len) return RdbError.UnexpectedEOF;

        const b = self.data[self.cursor];
        self.cursor += 1;

        var result: []const u8 = undefined;

        if ((b & 0x80) == 0) { // 0xxxxxxx: 7-bit uint
            result = try std.fmt.allocPrint(allocator, "{d}", .{b});
        } else if ((b & 0xC0) == 0x80) { // 10xxxxxx: 6-bit string length
            const len = b & 0x3F;
            const bytes = try self.readBytes(len);
            result = try allocator.dupe(u8, bytes);
        } else if ((b & 0xE0) == 0xC0) { // 110xxxxx: 13-bit int
            if (self.cursor >= self.data.len) return RdbError.UnexpectedEOF;
            const next = self.data[self.cursor];
            self.cursor += 1;
            const val = (@as(i16, @intCast(b & 0x1F)) << 8) | @as(i16, @intCast(next));
            result = try std.fmt.allocPrint(allocator, "{d}", .{val});
        } else if ((b & 0xF0) == 0xE0) { // 1110xxxx: 12-bit string len
            const high = b & 0x0F;
            if (self.cursor >= self.data.len) return RdbError.UnexpectedEOF;
            const low = self.data[self.cursor];
            self.cursor += 1;
            const len = (@as(usize, high) << 8) | @as(usize, low);
            const bytes = try self.readBytes(len);
            result = try allocator.dupe(u8, bytes);
        } else {
            switch (b) {
                0xF0 => {
                    const len = std.mem.readInt(u32, self.data[self.cursor..][0..4], .little);
                    self.cursor += 4;
                    const bytes = try self.readBytes(len);
                    result = try allocator.dupe(u8, bytes);
                },
                0xF1 => {
                    const val = std.mem.readInt(i16, self.data[self.cursor..][0..2], .little);
                    self.cursor += 2;
                    result = try std.fmt.allocPrint(allocator, "{d}", .{val});
                },
                0xF2 => {
                    const ptr = self.data[self.cursor..];
                    const val = @as(i32, ptr[0]) | (@as(i32, ptr[1]) << 8) | (@as(i32, @intCast(ptr[2])) << 16);
                    self.cursor += 3;
                    const sval = (val << 8) >> 8;
                    result = try std.fmt.allocPrint(allocator, "{d}", .{sval});
                },
                0xF3 => {
                    const val = std.mem.readInt(i32, self.data[self.cursor..][0..4], .little);
                    self.cursor += 4;
                    result = try std.fmt.allocPrint(allocator, "{d}", .{val});
                },
                0xF4 => {
                    const val = std.mem.readInt(i64, self.data[self.cursor..][0..8], .little);
                    self.cursor += 8;
                    result = try std.fmt.allocPrint(allocator, "{d}", .{val});
                },
                else => return RdbError.InvalidListpackEncoding,
            }
        }

        const element_len = self.cursor - start_cursor;
        self.skipBacklen(element_len);
        return result;
    }

    fn readBytes(self: *ListpackReader, len: usize) ![]const u8 {
        if (self.cursor + len > self.data.len) return RdbError.UnexpectedEOF;
        const s = self.data[self.cursor .. self.cursor + len];
        self.cursor += len;
        return s;
    }
};

fn lzfDecompress(in: []const u8, out: []u8) !void {
    var i: usize = 0;
    var o: usize = 0;

    while (i < in.len) {
        const ctrl = in[i];
        i += 1;

        if (ctrl < 32) {
            // Literal run
            const len = @as(usize, ctrl) + 1;
            if (o + len > out.len or i + len > in.len) return error.LzfOutputTooSmall;
            @memcpy(out[o .. o + len], in[i .. i + len]);
            i += len;
            o += len;
        } else {
            // Back reference
            var len = @as(usize, ctrl >> 5);
            if (len == 7) {
                if (i >= in.len) return error.LzfInputTruncated;
                len += in[i];
                i += 1;
            }
            // The match length is len + 2
            len += 2;

            if (i >= in.len) return error.LzfInputTruncated;
            const offset_low = in[i];
            i += 1;

            // Offset is (high << 8) | low + 1
            const offset = (@as(usize, ctrl & 0x1F) << 8) | offset_low;
            const ref_idx = if (o > offset) o - offset - 1 else return error.LzfInvalidReference;

            if (o + len > out.len) return error.LzfOutputTooSmall;

            // Copy byte by byte because source and dest may overlap
            var k: usize = 0;
            while (k < len) : (k += 1) {
                out[o + k] = out[ref_idx + k];
            }
            o += len;
        }
    }

    if (o != out.len) return error.LzfSizeMismatch;
}
