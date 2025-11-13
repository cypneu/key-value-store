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

    pub fn addEntry(self: *StreamStore, key: []const u8, entry_id: []const u8, field_values: []const FieldValue) !void {
        var gop = try self.data.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = .{};
        }

        const parsed_id = EntryId.parse(entry_id) catch return error.InvalidEntryId;
        try ensureValidEntryId(gop.value_ptr, parsed_id);

        var entry = try StreamEntry.init(self.allocator, entry_id, field_values);
        errdefer entry.deinit(self.allocator);

        try gop.value_ptr.appendEntry(self.allocator, entry);
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
