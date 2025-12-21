const std = @import("std");

pub const TransactionCommand = struct {
    raw: []const u8,
};

pub const TransactionState = struct {
    queue: std.ArrayList(TransactionCommand),
    had_error: bool,
};

pub const TransactionManager = struct {
    allocator: std.mem.Allocator,
    states: std.AutoHashMap(u64, TransactionState),

    pub fn init(allocator: std.mem.Allocator) TransactionManager {
        return .{
            .allocator = allocator,
            .states = std.AutoHashMap(u64, TransactionState).init(allocator),
        };
    }

    pub fn deinit(self: *TransactionManager) void {
        var it = self.states.iterator();
        while (it.next()) |entry| {
            self.clearQueue(entry.key_ptr.*);
            entry.value_ptr.queue.deinit();
        }
        self.states.deinit();
    }

    pub fn isInTransaction(self: *TransactionManager, connection_id: u64) bool {
        return self.states.contains(connection_id);
    }

    pub fn startTransaction(self: *TransactionManager, connection_id: u64) !void {
        const gop = try self.states.getOrPut(connection_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .queue = std.ArrayList(TransactionCommand).init(self.allocator),
                .had_error = false,
            };
        }
    }

    pub fn queueCommand(self: *TransactionManager, connection_id: u64, raw: []const u8) !void {
        const state = self.states.getPtr(connection_id) orelse return error.NotInTransaction;
        const stored_raw = try self.allocator.dupe(u8, raw);
        try state.queue.append(.{ .raw = stored_raw });
    }

    pub fn markError(self: *TransactionManager, connection_id: u64) void {
        if (self.states.getPtr(connection_id)) |state| {
            state.had_error = true;
        }
    }

    pub fn hasError(self: *TransactionManager, connection_id: u64) bool {
        const state = self.states.getPtr(connection_id) orelse return false;
        return state.had_error;
    }

    pub fn getQueue(self: *TransactionManager, connection_id: u64) ?[]const TransactionCommand {
        const state = self.states.getPtr(connection_id) orelse return null;
        return state.queue.items;
    }

    pub fn clearQueue(self: *TransactionManager, connection_id: u64) void {
        const state = self.states.getPtr(connection_id) orelse return;
        for (state.queue.items) |queued| {
            self.allocator.free(queued.raw);
        }
        state.queue.clearRetainingCapacity();
    }

    pub fn endTransaction(self: *TransactionManager, connection_id: u64) void {
        if (self.states.fetchRemove(connection_id)) |kv| {
            for (kv.value.queue.items) |queued| {
                self.allocator.free(queued.raw);
            }
            var queue = kv.value.queue;
            queue.deinit();
        }
    }
};
