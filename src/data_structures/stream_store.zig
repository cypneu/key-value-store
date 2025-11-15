const std = @import("std");

pub const FieldValue = struct {
    field: []const u8,
    value: []const u8,
};

pub const EntryField = struct {
    name: []const u8,
    value: []const u8,
};

const EntryKeyBytes: usize = 16;
const EntryKey = [EntryKeyBytes]u8;

const EntryId = struct {
    milliseconds: u64,
    sequence: u64,

    fn parse(entry_id: []const u8) !EntryId {
        const dash_index = std.mem.indexOfScalar(u8, entry_id, '-') orelse return error.InvalidEntryId;
        return try parseParts(entry_id, dash_index);
    }

    fn parseRangeBound(entry_id: []const u8, comptime is_start: bool) !EntryId {
        if (entry_id.len == 0) return error.InvalidEntryId;

        if (is_start and entry_id.len == 1 and entry_id[0] == '-') {
            return .{ .milliseconds = 0, .sequence = 0 };
        }
        if (!is_start and entry_id.len == 1 and entry_id[0] == '+') {
            return .{ .milliseconds = std.math.maxInt(u64), .sequence = std.math.maxInt(u64) };
        }

        if (std.mem.indexOfScalar(u8, entry_id, '-')) |dash_index| {
            return try parseParts(entry_id, dash_index);
        }

        const milliseconds = try parsePart(entry_id);
        const sequence: u64 = if (is_start) 0 else std.math.maxInt(u64);
        return .{ .milliseconds = milliseconds, .sequence = sequence };
    }

    pub fn format(self: EntryId, buffer: []u8) ![]const u8 {
        return try std.fmt.bufPrint(buffer, "{d}-{d}", .{ self.milliseconds, self.sequence });
    }

    fn isZero(self: EntryId) bool {
        return self.milliseconds == 0 and self.sequence == 0;
    }

    fn compare(self: EntryId, other: EntryId) std.math.Order {
        if (self.milliseconds < other.milliseconds) return .lt;
        if (self.milliseconds > other.milliseconds) return .gt;
        if (self.sequence < other.sequence) return .lt;
        if (self.sequence > other.sequence) return .gt;
        return .eq;
    }

    fn isGreaterThan(self: EntryId, other: EntryId) bool {
        return self.compare(other) == .gt;
    }

    fn isLessThan(self: EntryId, other: EntryId) bool {
        return self.compare(other) == .lt;
    }

    fn parsePart(slice: []const u8) !u64 {
        return std.fmt.parseInt(u64, slice, 10) catch return error.InvalidEntryId;
    }

    fn parseParts(entry_id: []const u8, dash_index: usize) !EntryId {
        if (dash_index == 0 or dash_index + 1 >= entry_id.len) return error.InvalidEntryId;

        return .{
            .milliseconds = try parsePart(entry_id[0..dash_index]),
            .sequence = try parsePart(entry_id[dash_index + 1 ..]),
        };
    }
};

pub const StreamEntry = struct {
    id: EntryId,
    fields: []EntryField,
    field_storage: []u8,

    fn init(allocator: std.mem.Allocator, id: EntryId, field_values: []const FieldValue) !StreamEntry {
        const fields_buffer = try allocator.alloc(EntryField, field_values.len);
        errdefer allocator.free(fields_buffer);

        const storage = try allocator.alloc(u8, totalFieldBytes(field_values));
        errdefer allocator.free(storage);

        var cursor: usize = 0;

        for (field_values, 0..) |pair, idx| {
            const name_slice = storage[cursor .. cursor + pair.field.len];
            @memcpy(name_slice, pair.field);
            cursor += pair.field.len;

            const value_slice = storage[cursor .. cursor + pair.value.len];
            @memcpy(value_slice, pair.value);
            cursor += pair.value.len;

            fields_buffer[idx] = .{ .name = name_slice, .value = value_slice };
        }

        return .{ .id = id, .fields = fields_buffer, .field_storage = storage };
    }

    fn deinit(self: StreamEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.fields);
        allocator.free(self.field_storage);
    }

    fn totalFieldBytes(field_values: []const FieldValue) usize {
        var total: usize = 0;
        for (field_values) |pair| {
            total += pair.field.len + pair.value.len;
        }
        return total;
    }
};

const RadixTree = struct {
    const Child = struct {
        label: []u8,
        node: *Node,
    };

    const Node = struct {
        children: std.ArrayListUnmanaged(Child) = .{},
        has_value: bool = false,
        value: StreamEntry = undefined,

        fn deinit(self: *Node, allocator: std.mem.Allocator) void {
            if (self.has_value) {
                self.value.deinit(allocator);
            }

            for (self.children.items) |child| {
                allocator.free(child.label);
                child.node.deinit(allocator);
                allocator.destroy(child.node);
            }

            self.children.deinit(allocator);
        }

        fn findChildIndex(self: *Node, label: u8) ?usize {
            for (self.children.items, 0..) |child, idx| {
                const first = child.label[0];
                if (first == label) return idx;
                if (first > label) break;
            }
            return null;
        }

        fn insertChild(self: *Node, allocator: std.mem.Allocator, label: []const u8) !*Node {
            const label_copy = try allocator.dupe(u8, label);
            errdefer allocator.free(label_copy);

            const child_node = try allocator.create(Node);
            errdefer allocator.destroy(child_node);
            child_node.* = .{};

            var idx: usize = 0;
            while (idx < self.children.items.len and self.children.items[idx].label[0] < label_copy[0]) : (idx += 1) {}

            try self.children.insert(allocator, idx, .{ .label = label_copy, .node = child_node });

            return child_node;
        }

        fn splitChild(self: *Node, allocator: std.mem.Allocator, child_index: usize, split_pos: usize) !*Node {
            var child = &self.children.items[child_index];

            const prefix = try allocator.dupe(u8, child.label[0..split_pos]);
            errdefer allocator.free(prefix);

            const suffix = try allocator.dupe(u8, child.label[split_pos..]);
            errdefer allocator.free(suffix);

            const mid_node = try allocator.create(Node);
            errdefer {
                mid_node.deinit(allocator);
                allocator.destroy(mid_node);
            }
            mid_node.* = .{};

            try mid_node.children.append(allocator, .{ .label = suffix, .node = child.node });

            allocator.free(child.label);
            child.label = prefix;
            child.node = mid_node;
            return mid_node;
        }
    };

    root: Node = .{},

    fn commonPrefixLength(a: []const u8, b: []const u8) usize {
        const limit = @min(a.len, b.len);
        var idx: usize = 0;
        while (idx < limit and a[idx] == b[idx]) : (idx += 1) {}
        return idx;
    }

    fn deinit(self: *RadixTree, allocator: std.mem.Allocator) void {
        self.root.deinit(allocator);
    }

    const InsertDecision = union(enum) {
        descend: struct { node: *Node, bytes: usize },
        split: struct { index: usize, common: usize },
        append: []const u8,
    };

    fn evaluateInsertDecision(node: *Node, key: []const u8, offset: usize) InsertDecision {
        if (node.findChildIndex(key[offset])) |child_idx| {
            const child = &node.children.items[child_idx];
            const remaining = key[offset..];
            const common = commonPrefixLength(child.label, remaining);

            if (common == child.label.len) {
                return .{ .descend = .{ .node = child.node, .bytes = common } };
            }

            return .{ .split = .{ .index = child_idx, .common = common } };
        }

        return .{ .append = key[offset..] };
    }

    fn insert(self: *RadixTree, allocator: std.mem.Allocator, key: []const u8, entry: StreamEntry) !*StreamEntry {
        var node = &self.root;
        var offset: usize = 0;

        while (offset < key.len) {
            switch (evaluateInsertDecision(node, key, offset)) {
                .descend => |step| {
                    node = step.node;
                    offset += step.bytes;
                },
                .split => |data| {
                    node = try node.splitChild(allocator, data.index, data.common);
                    offset += data.common;
                },
                .append => |remaining| {
                    node = try node.insertChild(allocator, remaining);
                    offset = key.len;
                },
            }
        }

        if (node.has_value) return error.DuplicateEntry;
        node.value = entry;
        node.has_value = true;
        return &node.value;
    }

    fn range(self: *RadixTree, allocator: std.mem.Allocator, start_key: EntryKey, end_key: EntryKey) ![]*const StreamEntry {
        if (std.mem.order(u8, start_key[0..], end_key[0..]) == .gt) {
            return &[_]*const StreamEntry{};
        }

        var results = std.ArrayList(*const StreamEntry).init(allocator);
        errdefer results.deinit();

        try collectRange(&self.root, 0, &start_key, &end_key, .equal, .equal, &results);

        return try results.toOwnedSlice();
    }

    const BoundCmp = enum { less, equal, greater };

    const ChildDecision = union(enum) {
        descend: struct {
            depth: usize,
            start_state: BoundCmp,
            end_state: BoundCmp,
        },
        before_start,
        after_end,
    };

    fn evaluateChildBounds(
        label: []const u8,
        depth: usize,
        start_key: *const EntryKey,
        end_key: *const EntryKey,
        start_state: BoundCmp,
        end_state: BoundCmp,
    ) !ChildDecision {
        if (depth + label.len > EntryKeyBytes) return error.InvalidEntryKey;
        if (start_state == .less) return .before_start;
        if (end_state == .greater) return .after_end;

        var cursor = depth;
        var start_cmp = start_state;
        var end_cmp = end_state;

        for (label) |byte| {
            if (start_cmp == .equal) {
                const bound = start_key.*[cursor];
                if (byte < bound) return .before_start;
                if (byte > bound) start_cmp = .greater;
            }

            if (end_cmp == .equal) {
                const bound = end_key.*[cursor];
                if (byte > bound) return .after_end;
                if (byte < bound) end_cmp = .less;
            }

            cursor += 1;
        }

        return .{ .descend = .{ .depth = cursor, .start_state = start_cmp, .end_state = end_cmp } };
    }

    fn collectRange(
        node: *Node,
        depth: usize,
        start_key: *const EntryKey,
        end_key: *const EntryKey,
        start_state: BoundCmp,
        end_state: BoundCmp,
        results: *std.ArrayList(*const StreamEntry),
    ) !void {
        if (node.has_value and depth == EntryKeyBytes and start_state != .less and end_state != .greater) {
            try results.append(&node.value);
        }

        if (depth == EntryKeyBytes) return;

        for (node.children.items) |child| {
            const decision = try evaluateChildBounds(child.label, depth, start_key, end_key, start_state, end_state);

            switch (decision) {
                .descend => |state| {
                    try collectRange(child.node, state.depth, start_key, end_key, state.start_state, state.end_state, results);
                },
                .before_start => {},
                .after_end => {},
            }
        }
    }
};

const Stream = struct {
    tree: RadixTree = .{},
    last_id: ?EntryId = null,

    fn deinit(self: *Stream, allocator: std.mem.Allocator) void {
        self.tree.deinit(allocator);
    }

    fn appendEntry(self: *Stream, allocator: std.mem.Allocator, entry_id: EntryId, entry: StreamEntry) !*StreamEntry {
        var key = entryKeyFromId(entry_id);
        const stored = try self.tree.insert(allocator, key[0..], entry);
        self.last_id = entry_id;
        return stored;
    }

    fn xrange(self: *Stream, allocator: std.mem.Allocator, start: EntryKey, end: EntryKey) ![]*const StreamEntry {
        return try self.tree.range(allocator, start, end);
    }
};

fn entryKeyFromId(entry_id: EntryId) EntryKey {
    var buf: EntryKey = undefined;
    std.mem.writeInt(u64, buf[0..8], entry_id.milliseconds, .big);
    std.mem.writeInt(u64, buf[8..16], entry_id.sequence, .big);
    return buf;
}

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

        if (stream.last_id) |last| {
            if (!entry_id.isGreaterThan(last)) return error.EntryIdTooSmall;
        }
    }

    fn nextSequence(stream: *Stream, milliseconds: u64) !u64 {
        if (stream.last_id) |last| {
            if (milliseconds == last.milliseconds) {
                return last.sequence + 1;
            }

            if (milliseconds < last.milliseconds) {
                return error.EntryIdTooSmall;
            }
        }

        return if (milliseconds == 0) 1 else 0;
    }

    fn generateAutoEntryId(stream: *Stream) EntryId {
        const now_ms_signed = std.time.milliTimestamp();
        const now_ms: u64 = if (now_ms_signed < 0) 0 else @intCast(now_ms_signed);

        var milliseconds = now_ms;
        var sequence: u64 = if (milliseconds == 0) 1 else 0;

        if (stream.last_id) |last| {
            if (milliseconds <= last.milliseconds) {
                milliseconds = last.milliseconds;
                sequence = last.sequence + 1;
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

    pub fn addEntry(
        self: *StreamStore,
        result_allocator: std.mem.Allocator,
        key: []const u8,
        entry_id: []const u8,
        field_values: []const FieldValue,
    ) ![]const u8 {
        var maybe_stream = self.data.getPtr(key);

        var scratch_stream: Stream = .{};
        const id_stream: *Stream = maybe_stream orelse &scratch_stream;

        var id_buffer: [64]u8 = undefined;
        const resolved_entry_id = try resolveEntryId(id_stream, entry_id, id_buffer[0..]);

        const parsed_id = EntryId.parse(resolved_entry_id) catch return error.InvalidEntryId;
        try ensureValidEntryId(id_stream, parsed_id);

        var entry = try StreamEntry.init(self.allocator, parsed_id, field_values);
        errdefer entry.deinit(self.allocator);

        if (maybe_stream == null) {
            const gop = try self.data.getOrPut(key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try self.allocator.dupe(u8, key);
                gop.value_ptr.* = .{};
            }
            maybe_stream = gop.value_ptr;
        }

        _ = try maybe_stream.?.appendEntry(self.allocator, parsed_id, entry);

        const formatted_id = try parsed_id.format(id_buffer[0..]);
        return try result_allocator.dupe(u8, formatted_id);
    }

    pub fn xrange(self: *StreamStore, allocator: std.mem.Allocator, key: []const u8, start: []const u8, end: []const u8) ![]*const StreamEntry {
        const stream_ptr = self.data.getPtr(key) orelse return &[_]*const StreamEntry{};

        const start_id = EntryId.parseRangeBound(start, true) catch return error.InvalidRangeId;
        const end_id = EntryId.parseRangeBound(end, false) catch return error.InvalidRangeId;

        if (end_id.isLessThan(start_id)) {
            return &[_]*const StreamEntry{};
        }

        const start_key = entryKeyFromId(start_id);
        const end_key = entryKeyFromId(end_id);
        return try stream_ptr.xrange(allocator, start_key, end_key);
    }

    pub fn xread(self: *StreamStore, allocator: std.mem.Allocator, key: []const u8, start: []const u8) ![]*const StreamEntry {
        const stream_ptr = self.data.getPtr(key) orelse return &[_]*const StreamEntry{};

        const start_id = EntryId.parse(start) catch return error.InvalidRangeId;
        const start_key = entryKeyFromId(start_id);
        const end_id = EntryId{ .milliseconds = std.math.maxInt(u64), .sequence = std.math.maxInt(u64) };
        const end_key = entryKeyFromId(end_id);

        const entries = try stream_ptr.xrange(allocator, start_key, end_key);
        var idx: usize = 0;
        while (idx < entries.len) : (idx += 1) {
            if (entries[idx].*.id.isGreaterThan(start_id)) break;
        }

        return entries[idx..];
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
