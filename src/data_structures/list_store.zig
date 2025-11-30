const std = @import("std");

pub const ListStore = struct {
    allocator: std.mem.Allocator,
    data: std.StringHashMap(Deque([]const u8)),

    pub fn init(allocator: std.mem.Allocator) ListStore {
        return ListStore{
            .allocator = allocator,
            .data = std.StringHashMap(Deque([]const u8)).init(allocator),
        };
    }

    pub fn deinit(self: *ListStore) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.data.deinit();
    }

    pub fn prepend(self: *ListStore, key: []const u8, value: []const u8) !u64 {
        return self.push(key, value, true);
    }

    pub fn append(self: *ListStore, key: []const u8, value: []const u8) !u64 {
        return self.push(key, value, false);
    }

    pub fn push(self: *ListStore, key: []const u8, value: []const u8, comptime front: bool) !u64 {
        const owned = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned);

        var gop = try self.data.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = try Deque([]const u8).init(self.allocator, 32);
        }

        if (front)
            try gop.value_ptr.pushFront(owned)
        else
            try gop.value_ptr.pushBack(owned);

        return gop.value_ptr.len;
    }

    pub fn lrange(self: *ListStore, key: []const u8, start_index: i64, end_index: i64) RangeView([]const u8) {
        const deque = self.data.get(key) orelse return .{ .first = &[_][]const u8{}, .second = &[_][]const u8{} };

        const length = @as(i64, @intCast(deque.len));
        if (length == 0) return .{ .first = &[_][]const u8{}, .second = &[_][]const u8{} };

        var start = if (start_index < 0) start_index + length else start_index;
        var end = if (end_index < 0) end_index + length else end_index;

        start = @max(0, start);
        end = @min(length - 1, end);

        if (start > end) return .{ .first = &[_][]const u8{}, .second = &[_][]const u8{} };

        const first = @as(usize, @intCast(start));
        const last = @as(usize, @intCast(end + 1));

        return deque.range(first, last - first);
    }

    pub fn length_by_key(self: *ListStore, key: []const u8) u64 {
        return if (self.data.get(key)) |value| @as(u64, @intCast(value.len)) else 0;
    }

    pub fn lpop(self: *ListStore, key: []const u8, count: u64, out_allocator: std.mem.Allocator) ![][]const u8 {
        var entry = self.data.getEntry(key) orelse return &[_][]const u8{};

        const take = @min(count, entry.value_ptr.len);
        if (take == 0) return &[_][]const u8{};

        const output = try out_allocator.alloc([]const u8, take);

        for (output) |*slot| {
            const val = entry.value_ptr.popFront().?;
            slot.* = try out_allocator.dupe(u8, val);
            self.allocator.free(val);
        }

        if (entry.value_ptr.len == 0) {
            self.delete(key);
        }

        return output;
    }

    pub fn delete(self: *ListStore, key: []const u8) void {
        if (self.data.fetchRemove(key)) |removed| {
            var deque = removed.value;
            deque.deinit();
            self.allocator.free(removed.key);
        }
    }

    pub fn contains(self: *ListStore, key: []const u8) bool {
        return self.data.get(key) != null;
    }

    pub fn keys(self: *ListStore, allocator: std.mem.Allocator) ![][]const u8 {
        var list = std.ArrayList([]const u8).init(allocator);
        errdefer list.deinit();

        var it = self.data.iterator();
        while (it.next()) |entry| {
            try list.append(entry.key_ptr.*);
        }

        return list.toOwnedSlice();
    }

    pub fn clear(self: *ListStore) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.data.clearRetainingCapacity();
    }
};

pub fn RangeView(comptime T: type) type {
    return struct {
        first: []T,
        second: []T,
    };
}

fn Deque(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        buffer: []T,
        head: usize = 0,
        len: usize = 0,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, initial_capacity: usize) !Self {
            const capacity = @max(1, initial_capacity);
            return Self{
                .allocator = allocator,
                .buffer = try allocator.alloc(T, capacity),
            };
        }

        pub fn deinit(self: *Self) void {
            while (self.popFront()) |value| {
                self.allocator.free(value);
            }
            self.allocator.free(self.buffer);
        }

        pub fn pushBack(self: *Self, value: T) !void {
            try self.ensureCapacity(1);
            const idx = (self.head + self.len) % self.buffer.len;
            self.buffer[idx] = value;
            self.len += 1;
        }

        pub fn pushFront(self: *Self, value: T) !void {
            try self.ensureCapacity(1);
            self.head = (self.head + self.buffer.len - 1) % self.buffer.len;
            self.buffer[self.head] = value;
            self.len += 1;
        }

        pub fn popFront(self: *Self) ?T {
            if (self.len == 0) return null;
            const val = self.buffer[self.head];
            self.head = (self.head + 1) % self.buffer.len;
            self.len -= 1;
            return val;
        }

        pub fn popBack(self: *Self) ?T {
            if (self.len == 0) return null;
            const tail_idx = (self.head + self.len - 1) % self.buffer.len;
            const val = self.buffer[tail_idx];
            self.len -= 1;
            return val;
        }

        pub fn range(self: *const Self, start: usize, count: usize) RangeView(T) {
            const first = (self.head + start) % self.buffer.len;
            const end = (first + count);

            if (end <= self.buffer.len) {
                return .{
                    .first = self.buffer[first..end],
                    .second = self.buffer[0..0],
                };
            } else {
                const first_part = self.buffer[first..];
                const second_len = count - first_part.len;
                return .{
                    .first = first_part,
                    .second = self.buffer[0..second_len],
                };
            }
        }

        fn ensureCapacity(self: *Self, need_extra: usize) !void {
            if (self.len + need_extra <= self.buffer.len) return;

            const new_capacity = self.buffer.len * 2;
            var new_buffer = try self.allocator.alloc(T, new_capacity);

            const first_chunk = self.buffer[self.head..];
            const second_len = self.len - first_chunk.len;
            @memcpy(new_buffer[0..first_chunk.len], first_chunk);
            @memcpy(new_buffer[first_chunk.len..self.len], self.buffer[0..second_len]);

            self.allocator.free(self.buffer);
            self.buffer = new_buffer;
            self.head = 0;
        }
    };
}
