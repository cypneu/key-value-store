const std = @import("std");

pub const FieldValue = struct {
    field: []const u8,
    value: []const u8,
};

const EntryField = struct {
    name: []const u8,
    value: []const u8,
};

const EntryId = struct {
    milliseconds: u64,
    sequence: u64,

    fn parse(entry_id: []const u8) !EntryId {
        const dash_index = std.mem.indexOfScalar(u8, entry_id, '-') orelse return error.InvalidEntryId;
        if (dash_index == 0 or dash_index + 1 >= entry_id.len) return error.InvalidEntryId;

        const ms_slice = entry_id[0..dash_index];
        const seq_slice = entry_id[dash_index + 1 ..];

        const milliseconds = std.fmt.parseInt(u64, ms_slice, 10) catch return error.InvalidEntryId;
        const sequence = std.fmt.parseInt(u64, seq_slice, 10) catch return error.InvalidEntryId;

        return .{ .milliseconds = milliseconds, .sequence = sequence };
    }

    fn isZero(self: EntryId) bool {
        return self.milliseconds == 0 and self.sequence == 0;
    }

    fn isGreaterThan(self: EntryId, other: EntryId) bool {
        if (self.milliseconds != other.milliseconds) {
            return self.milliseconds > other.milliseconds;
        }
        return self.sequence > other.sequence;
    }
};

const StreamEntry = struct {
    id: []const u8,
    fields: []EntryField,

    fn init(allocator: std.mem.Allocator, entry_id: []const u8, field_values: []const FieldValue) !StreamEntry {
        const id_copy = try allocator.dupe(u8, entry_id);
        errdefer allocator.free(id_copy);

        const count = field_values.len;
        const fields_buffer = try allocator.alloc(EntryField, count);
        var filled: usize = 0;
        errdefer {
            for (fields_buffer[0..filled]) |field| {
                allocator.free(field.name);
                allocator.free(field.value);
            }
            allocator.free(fields_buffer);
            allocator.free(id_copy);
        }

        while (filled < count) {
            const pair = field_values[filled];
            const name = try allocator.dupe(u8, pair.field);
            const value = allocator.dupe(u8, pair.value) catch |err| {
                allocator.free(name);
                return err;
            };

            fields_buffer[filled] = .{ .name = name, .value = value };
            filled += 1;
        }

        return .{ .id = id_copy, .fields = fields_buffer };
    }

    fn deinit(self: StreamEntry, allocator: std.mem.Allocator) void {
        for (self.fields) |field| {
            allocator.free(field.name);
            allocator.free(field.value);
        }
        allocator.free(self.fields);
        allocator.free(self.id);
    }
};

const Stream = struct {
    entries: std.ArrayListUnmanaged(StreamEntry) = .{},

    fn deinit(self: *Stream, allocator: std.mem.Allocator) void {
        for (self.entries.items) |entry| {
            entry.deinit(allocator);
        }
        self.entries.deinit(allocator);
    }

    fn appendEntry(self: *Stream, allocator: std.mem.Allocator, entry: StreamEntry) !void {
        try self.entries.append(allocator, entry);
    }
};

pub const StreamStore = struct {
    allocator: std.mem.Allocator,
    data: std.StringHashMap(Stream),

    pub fn init(allocator: std.mem.Allocator) StreamStore {
        return .{
            .allocator = allocator,
            .data = std.StringHashMap(Stream).init(allocator),
        };
    }

    pub fn deinit(self: *StreamStore) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.data.deinit();
    }

    fn ensureValidEntryId(stream: *Stream, entry_id: EntryId) !void {
        if (entry_id.isZero()) return error.EntryIdZero;

        const entries = stream.entries.items;
        if (entries.len == 0) return;

        const last_entry = entries[entries.len - 1];
        const last_id = EntryId.parse(last_entry.id) catch unreachable;
        if (!entry_id.isGreaterThan(last_id)) return error.EntryIdTooSmall;
    }

    fn nextSequence(stream: *Stream, milliseconds: u64) !u64 {
        const entries = stream.entries.items;
        if (entries.len == 0) {
            return if (milliseconds == 0) 1 else 0;
        }

        const last_entry = entries[entries.len - 1];
        const last_id = EntryId.parse(last_entry.id) catch unreachable;

        if (milliseconds == last_id.milliseconds) {
            return last_id.sequence + 1;
        }

        if (milliseconds < last_id.milliseconds) {
            return error.EntryIdTooSmall;
        }

        return if (milliseconds == 0) 1 else 0;
    }

    fn generateAutoEntryId(stream: *Stream) EntryId {
        const now_ms_signed = std.time.milliTimestamp();
        const now_ms: u64 = if (now_ms_signed < 0) 0 else @intCast(now_ms_signed);

        var milliseconds = now_ms;
        var sequence: u64 = if (milliseconds == 0) 1 else 0;

        const entries = stream.entries.items;
        if (entries.len > 0) {
            const last_entry = entries[entries.len - 1];
            const last_id = EntryId.parse(last_entry.id) catch unreachable;

            if (milliseconds <= last_id.milliseconds) {
                milliseconds = last_id.milliseconds;
                sequence = last_id.sequence + 1;
            }
        }

        return .{ .milliseconds = milliseconds, .sequence = sequence };
    }

    fn resolveEntryId(stream: *Stream, entry_id: []const u8, buffer: []u8) ![]const u8 {
        if (entry_id.len == 1 and entry_id[0] == '*') {
            const auto_id = generateAutoEntryId(stream);
            return try std.fmt.bufPrint(buffer, "{d}-{d}", .{ auto_id.milliseconds, auto_id.sequence });
        }

        const dash_index = std.mem.indexOfScalar(u8, entry_id, '-') orelse return error.InvalidEntryId;
        if (dash_index == 0 or dash_index + 1 >= entry_id.len) return error.InvalidEntryId;

        const seq_slice = entry_id[dash_index + 1 ..];
        if (seq_slice.len == 1 and seq_slice[0] == '*') {
            const ms_slice = entry_id[0..dash_index];
            const milliseconds = std.fmt.parseInt(u64, ms_slice, 10) catch return error.InvalidEntryId;
            const sequence = try nextSequence(stream, milliseconds);

            return try std.fmt.bufPrint(buffer, "{d}-{d}", .{ milliseconds, sequence });
        }

        return entry_id;
    }

    pub fn addEntry(self: *StreamStore, key: []const u8, entry_id: []const u8, field_values: []const FieldValue) ![]const u8 {
        var gop = try self.data.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = .{};
        }

        var id_buffer: [64]u8 = undefined;
        const resolved_entry_id = try resolveEntryId(gop.value_ptr, entry_id, id_buffer[0..]);

        const parsed_id = EntryId.parse(resolved_entry_id) catch return error.InvalidEntryId;
        try ensureValidEntryId(gop.value_ptr, parsed_id);

        var entry = try StreamEntry.init(self.allocator, resolved_entry_id, field_values);
        errdefer entry.deinit(self.allocator);

        try gop.value_ptr.appendEntry(self.allocator, entry);
        const stored_entry = gop.value_ptr.entries.items[gop.value_ptr.entries.items.len - 1];
        return stored_entry.id;
    }

    pub fn contains(self: *StreamStore, key: []const u8) bool {
        return self.data.get(key) != null;
    }

    pub fn delete(self: *StreamStore, key: []const u8) void {
        if (self.data.fetchRemove(key)) |removed| {
            var stream = removed.value;
            stream.deinit(self.allocator);
            self.allocator.free(removed.key);
        }
    }
};
