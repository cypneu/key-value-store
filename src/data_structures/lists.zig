const std = @import("std");

pub const Lists = struct {
    allocator: std.mem.Allocator,
    data: std.StringHashMap(std.ArrayList([]const u8)),

    pub fn init(allocator: std.mem.Allocator) Lists {
        return Lists{
            .allocator = allocator,
            .data = std.StringHashMap(std.ArrayList([]const u8)).init(allocator),
        };
    }

    pub fn deinit(self: *Lists) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            for (entry.value_ptr.items) |item_slice| {
                self.allocator.free(item_slice);
            }
            entry.value_ptr.deinit();
        }
        self.data.deinit();
    }

    pub fn append(self: *Lists, key: []const u8, value: []const u8) !u64 {
        const gop = try self.data.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = std.ArrayList([]const u8).init(self.allocator);
        }

        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        const list = gop.value_ptr;
        try list.append(owned_value);
        return list.items.len;
    }

    pub fn lrange(self: *Lists, key: []const u8, start_index: i64, end_index: i64) []const []const u8 {
        const list = self.data.get(key) orelse return &[_][]const u8{};

        const length = @as(i64, @intCast(list.items.len));
        if (length == 0) return &[_][]const u8{};

        var start = if (start_index < 0) start_index + length else start_index;
        var end = if (end_index < 0) end_index + length else end_index;

        start = @max(0, start);
        end = @min(length - 1, end);

        if (start > end) return &[_][]const u8{};

        const first = @as(usize, @intCast(start));
        const last = @as(usize, @intCast(end + 1));

        return list.items[first..last];
    }
};
