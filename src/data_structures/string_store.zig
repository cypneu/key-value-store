const std = @import("std");

const Value = struct {
    data: []const u8,
    expiration_us: ?i64,

    fn isExpired(self: Value) bool {
        const expiration = self.expiration_us orelse return false;
        return std.time.microTimestamp() >= expiration;
    }
};

pub const StringStore = struct {
    allocator: std.mem.Allocator,
    data: std.StringHashMap(Value),

    pub fn init(allocator: std.mem.Allocator) StringStore {
        return StringStore{
            .allocator = allocator,
            .data = std.StringHashMap(Value).init(allocator),
        };
    }

    pub fn deinit(self: *StringStore) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.data);
        }
        self.data.deinit();
    }

    pub fn set(self: *StringStore, key: []const u8, value_slice: []const u8, expiration_us: ?i64) !void {
        const owned_value = try self.allocator.dupe(u8, value_slice);
        errdefer self.allocator.free(owned_value);

        const new_value = Value{
            .data = owned_value,
            .expiration_us = expiration_us,
        };

        if (try self.data.fetchPut(key, new_value)) |old_entry| {
            self.allocator.free(old_entry.value.data);
        }
    }

    pub fn get(self: *StringStore, key: []const u8) ?[]const u8 {
        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired()) {
            if (self.data.fetchRemove(key)) |removed| {
                self.allocator.free(removed.value.data);
            }
            return null;
        }

        return entry.value_ptr.data;
    }
};
