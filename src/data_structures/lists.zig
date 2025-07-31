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
            for (entry.value_ptr.items) |item_slice| {
                self.allocator.free(item_slice);
            }
            entry.value_ptr.deinit();
        }
        self.data.deinit();
    }

    pub fn append(self: *Lists, key: []const u8, value: []const u8) !u64 {
        const gop = try self.data.getOrPut(key);

        const list = gop.value_ptr;
        if (!gop.found_existing) {
            list.* = std.ArrayList([]const u8).init(self.allocator);
        }

        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        try list.append(owned_value);
        return list.items.len;
    }
};

