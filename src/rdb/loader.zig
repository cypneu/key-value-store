const std = @import("std");
const db = @import("../data_structures/mod.zig");
const types = @import("types.zig");
const constants = @import("constants.zig");
const lzf = @import("lzf.zig");
const ListpackReader = @import("listpack.zig").ListpackReader;

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

pub fn loadRDBFromBytes(allocator: std.mem.Allocator, bytes: []const u8, handler: anytype) !void {
    var stream = std.io.fixedBufferStream(bytes);
    var loader = RdbLoader(@TypeOf(stream.reader()), @TypeOf(handler)).init(allocator, stream.reader(), handler);
    try loader.load();
}

fn RdbLoader(comptime ReaderType: type, comptime Handler: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: ReaderType,
        handler: Handler,
        peeked_byte: ?u8 = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType, handler: Handler) Self {
            return .{
                .allocator = allocator,
                .reader = reader,
                .handler = handler,
            };
        }

        pub fn readByte(self: *Self) !u8 {
            if (self.peeked_byte) |b| {
                self.peeked_byte = null;
                return b;
            }
            return self.reader.readByte();
        }

        pub fn peekByte(self: *Self, byte: u8) void {
            self.peeked_byte = byte;
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
                    else => {},
                }
            }
        }

        fn verifyHeader(self: *Self) !void {
            var header: [9]u8 = undefined;
            const bytes_read = try self.reader.readAll(&header);
            if (bytes_read != 9 or !std.mem.eql(u8, header[0..5], "REDIS")) {
                return types.RdbError.InvalidHeader;
            }
        }

        fn parseMetadata(self: *Self) !void {
            const key = try StringReader.read(self);
            defer self.allocator.free(key);
            const val = try StringReader.read(self);
            defer self.allocator.free(val);
        }

        fn parseDatabase(self: *Self) !void {
            _ = try LengthReader.read(self); // Database index

            const byte = try self.readByte();
            if (byte == 0xFB) {
                _ = try LengthReader.read(self); // hash table size
                _ = try LengthReader.read(self); // expire hash table size
            } else {
                self.peekByte(byte);
            }

            while (try self.parseKeyValuePair()) {}
        }

        fn parseKeyValuePair(self: *Self) !bool {
            const opcode = self.readByte() catch |err| {
                if (err == error.EndOfStream) return false;
                return err;
            };

            switch (opcode) {
                0xFF, 0xFE => {
                    self.peekByte(opcode);
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
            const key = try StringReader.read(self);
            defer self.allocator.free(key);

            var expire_us: ?i64 = null;
            if (expire_ms) |ms| {
                expire_us = @as(i64, @intCast(ms)) * 1000;
            }

            switch (value_type) {
                @intFromEnum(constants.ValueType.string) => {
                    const val = try StringReader.read(self);
                    defer self.allocator.free(val);
                    try self.handler.string_store.set(key, val, expire_us);
                },
                @intFromEnum(constants.ValueType.list_quicklist_2) => try self.loadList(key),
                @intFromEnum(constants.ValueType.stream_listpacks_3) => try self.loadStream(key),
                else => return types.RdbError.UnsupportedEncoding,
            }
            return true;
        }

        fn loadList(self: *Self, key: []const u8) !void {
            const len_info = try LengthReader.read(self);
            var i: u64 = 0;
            while (i < len_info.len) : (i += 1) {
                const container = try LengthReader.read(self); // Container ID
                if (container.len != 2) return types.RdbError.UnsupportedEncoding;

                const blob = try StringReader.read(self);
                defer self.allocator.free(blob);

                var lp = ListpackReader.init(blob);
                while (!lp.isAtEnd()) {
                    const val = try lp.readStringOwned(self.allocator);
                    defer self.allocator.free(val);
                    _ = try self.handler.list_store.append(key, val);
                }
            }
        }

        fn loadStream(self: *Self, key: []const u8) !void {
            var stream_loader = StreamLoader(Self).init(self, key);
            try stream_loader.load();
        }
    };
}

fn StreamLoader(comptime Loader: type) type {
    return struct {
        loader: *Loader,
        key: []const u8,
        allocator: std.mem.Allocator,

        const Self = @This();

        const Flags = struct {
            const Deleted = 1 << 0;
            const SameFields = 1 << 1;
        };

        pub fn init(loader: *Loader, key: []const u8) Self {
            return .{
                .loader = loader,
                .key = key,
                .allocator = loader.allocator,
            };
        }

        pub fn load(self: *Self) !void {
            try self.loadData();
            try self.loadMetadata();
            try self.loadConsumerGroups();
        }

        fn loadData(self: *Self) !void {
            const listpacks_count = try LengthReader.read(self.loader);
            var i: u64 = 0;
            while (i < listpacks_count.len) : (i += 1) {
                const master_id_str = try StringReader.read(self.loader);
                defer self.allocator.free(master_id_str);

                if (master_id_str.len != 16) return types.RdbError.UnsupportedEncoding;

                const ms_base = std.mem.readInt(u64, master_id_str[0..8], .big);
                const seq_base = std.mem.readInt(u64, master_id_str[8..16], .big);

                const listpack_blob = try StringReader.read(self.loader);
                defer self.allocator.free(listpack_blob);

                var lp = ListpackReader.init(listpack_blob);
                try self.processListpack(&lp, ms_base, seq_base);
            }
        }

        fn processListpack(self: *Self, lp: *ListpackReader, ms_base: u64, seq_base: u64) !void {
            const count = try lp.readInt();
            const deleted = try lp.readInt();
            const num_master_fields = try lp.readInt();
            const total_entries = count + deleted;

            var master_fields = try std.ArrayList([]const u8).initCapacity(self.allocator, @intCast(num_master_fields));
            defer {
                for (master_fields.items) |f| self.allocator.free(f);
                master_fields.deinit();
            }

            var mf_i: u64 = 0;
            while (mf_i < num_master_fields) : (mf_i += 1) {
                const field = try lp.readStringOwned(self.allocator);
                master_fields.appendAssumeCapacity(field);
            }

            _ = try lp.readInt(); // Zero terminator

            var i: u64 = 0;
            while (i < total_entries) : (i += 1) {
                try self.processEntry(lp, ms_base, seq_base, master_fields.items);
            }
        }

        fn processEntry(self: *Self, lp: *ListpackReader, ms_base: u64, seq_base: u64, master_fields: []const []const u8) !void {
            const flags = try lp.readInt();
            const ms_delta = try lp.readInt();
            const seq_delta = try lp.readInt();

            const is_deleted = (flags & Flags.Deleted) != 0;
            const use_master_fields = (flags & Flags.SameFields) != 0;

            var fields = try self.readEntryFields(lp, use_master_fields, master_fields);
            defer {
                for (fields.items) |fv| {
                    if (!use_master_fields) self.allocator.free(fv.field);
                    self.allocator.free(fv.value);
                }
                fields.deinit();
            }

            _ = try lp.readInt(); // lp-count

            if (!is_deleted) {
                var id_buf: [64]u8 = undefined;
                const id_str = try self.reconstructId(ms_base, seq_base, ms_delta, seq_delta, &id_buf);
                const added_id = try self.loader.handler.stream_store.addEntry(self.allocator, self.key, id_str, fields.items);
                self.allocator.free(added_id);
            }
        }

        fn readEntryFields(self: *Self, lp: *ListpackReader, use_master: bool, master_fields: []const []const u8) !std.ArrayList(db.StreamFieldValue) {
            var fields = std.ArrayList(db.StreamFieldValue).init(self.allocator);
            errdefer fields.deinit();

            if (use_master) {
                for (master_fields) |name| {
                    const val = try lp.readStringOwned(self.allocator);
                    try fields.append(.{ .field = name, .value = val });
                }
            } else {
                const num_fields = try lp.readInt();
                var i: u64 = 0;
                while (i < num_fields) : (i += 1) {
                    const name = try lp.readStringOwned(self.allocator);
                    const val = try lp.readStringOwned(self.allocator);
                    try fields.append(.{ .field = name, .value = val });
                }
            }
            return fields;
        }

        fn reconstructId(self: *Self, ms_base: u64, seq_base: u64, ms_delta: i64, seq_delta: i64, buf: []u8) ![]u8 {
            _ = self;
            const ms = ms_base + @as(u64, @intCast(ms_delta));
            const seq = seq_base + @as(u64, @intCast(seq_delta));
            return std.fmt.bufPrint(buf, "{d}-{d}", .{ ms, seq });
        }

        fn loadMetadata(self: *Self) !void {
            _ = try LengthReader.read(self.loader); // Total items
            _ = try LengthReader.read(self.loader); // Last ID ms
            _ = try LengthReader.read(self.loader); // Last ID seq
            _ = try LengthReader.read(self.loader); // First ID ms
            _ = try LengthReader.read(self.loader); // First ID seq
            _ = try LengthReader.read(self.loader); // Max Deleted ID ms
            _ = try LengthReader.read(self.loader); // Max Deleted ID seq
            _ = try LengthReader.read(self.loader); // Entries Added
        }

        fn loadConsumerGroups(self: *Self) !void {
            const cgroups_count = try LengthReader.read(self.loader);
            var cg_i: u64 = 0;
            while (cg_i < cgroups_count.len) : (cg_i += 1) {
                const cg_name = try StringReader.read(self.loader);
                self.allocator.free(cg_name);

                _ = try LengthReader.read(self.loader); // Last CG ID ms
                _ = try LengthReader.read(self.loader); // Last CG ID seq

                try self.skipStreamPEL();

                const consumers_count = try LengthReader.read(self.loader);
                var c_i: u64 = 0;
                while (c_i < consumers_count.len) : (c_i += 1) {
                    const consumer_name = try StringReader.read(self.loader);
                    self.allocator.free(consumer_name);

                    _ = try self.loader.reader.readInt(u64, .little);
                    try self.skipStreamPEL();
                }
            }
        }

        fn skipStreamPEL(self: *Self) !void {
            const pending_count = try LengthReader.read(self.loader);
            var i: u64 = 0;
            while (i < pending_count.len) : (i += 1) {
                var raw_id: [16]u8 = undefined;
                try self.loader.reader.readNoEof(&raw_id);
                _ = try self.loader.reader.readInt(u64, .little);
                _ = try LengthReader.read(self.loader);
            }
        }
    };
}

const LengthReader = struct {
    pub const Info = struct {
        len: u64,
        is_encoded: bool,
        encoding_type: u6,
    };

    pub fn read(loader: anytype) !Info {
        const byte = try loader.readByte();
        const type_bits = (byte & 0xC0) >> 6;
        const remaining_bits = byte & 0x3F;

        return switch (type_bits) {
            0b00 => .{ .len = remaining_bits, .is_encoded = false, .encoding_type = 0 },
            0b01 => read14Bit(loader, remaining_bits),
            0b10 => readLarge(loader, remaining_bits),
            0b11 => .{ .len = 0, .is_encoded = true, .encoding_type = @truncate(remaining_bits) },
            else => unreachable,
        };
    }

    fn read14Bit(loader: anytype, high_bits: u8) !Info {
        const next_byte = try loader.readByte();
        const len = (@as(u64, high_bits) << 8) | next_byte;
        return .{ .len = len, .is_encoded = false, .encoding_type = 0 };
    }

    fn readLarge(loader: anytype, subtype: u8) !Info {
        const len = switch (subtype) {
            0 => try loader.reader.readInt(u32, .big),
            1 => try loader.reader.readInt(u64, .big),
            else => return types.RdbError.InvalidLengthEncoding,
        };
        return .{ .len = len, .is_encoded = false, .encoding_type = 0 };
    }
};

const StringReader = struct {
    pub fn read(loader: anytype) ![]u8 {
        const length_info = try LengthReader.read(loader);

        if (!length_info.is_encoded) {
            return readRaw(loader, length_info.len);
        }

        return switch (length_info.encoding_type) {
            0, 1, 2 => readInteger(loader, length_info.encoding_type),
            3 => readLzf(loader),
            else => types.RdbError.UnsupportedEncoding,
        };
    }

    fn readRaw(loader: anytype, len: u64) ![]u8 {
        const buf = try loader.allocator.alloc(u8, len);
        errdefer loader.allocator.free(buf);
        try loader.reader.readNoEof(buf);
        return buf;
    }

    fn readInteger(loader: anytype, encoding_type: u6) ![]u8 {
        const val: u32 = switch (encoding_type) {
            0 => try loader.readByte(),
            1 => try loader.reader.readInt(u16, .little),
            2 => try loader.reader.readInt(u32, .little),
            else => return types.RdbError.UnsupportedEncoding,
        };
        return try std.fmt.allocPrint(loader.allocator, "{d}", .{val});
    }

    fn readLzf(loader: anytype) ![]u8 {
        const clen_info = try LengthReader.read(loader);
        const ulen_info = try LengthReader.read(loader);

        const compressed = try loader.allocator.alloc(u8, clen_info.len);
        defer loader.allocator.free(compressed);
        try loader.reader.readNoEof(compressed);

        const uncompressed = try loader.allocator.alloc(u8, ulen_info.len);
        errdefer loader.allocator.free(uncompressed);

        lzf.decompress(compressed, uncompressed) catch {
            return types.RdbError.UnsupportedEncoding;
        };

        return uncompressed;
    }
};
