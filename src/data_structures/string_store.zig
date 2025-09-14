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
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.data);
        }
        self.data.deinit();
    }

    pub fn set(self: *StringStore, key: []const u8, value_slice: []const u8, expiration_us: ?i64) !void {
        const owned_value = try self.allocator.dupe(u8, value_slice);
        errdefer self.allocator.free(owned_value);

        const gop = try self.data.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = .{ .data = owned_value, .expiration_us = expiration_us };
        } else {
            self.allocator.free(gop.value_ptr.data);
            gop.value_ptr.* = .{ .data = owned_value, .expiration_us = expiration_us };
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
