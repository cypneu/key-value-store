const std = @import("std");
const reply_mod = @import("../commands/reply.zig");
const Reply = reply_mod.Reply;
const writeReply = @import("../network/resp/writer.zig").writeReply;
const Order = std.math.Order;

const services = @import("../services.zig");
const command = @import("../commands/command.zig");

pub const WriteOp = services.WriteOp;

pub const TimeoutEntry = struct {
    deadline_us: i64,
    connection_id: u64,
};

const StreamReadRequest = command.StreamReadRequest;

pub const StreamRegistration = struct {
    key: []const u8,
    node: *std.DoublyLinkedList(u64).Node,
};

pub const StreamBlockingState = struct {
    requests: []StreamReadRequest,
    registrations: []StreamRegistration,
};

pub const WaitState = struct {
    required: u64,
    target_offset: u64,
};

pub const ListBlockingState = struct {
    key: []const u8,
    node: *std.DoublyLinkedList(u64).Node,
};

pub const BlockedState = union(enum) {
    List: ListBlockingState,
    Stream: StreamBlockingState,
    Wait: WaitState,
};

pub const DrainResult = struct {
    ops: []WriteOp,
    popped: usize,
};

fn timeoutCompare(_: void, a: TimeoutEntry, b: TimeoutEntry) Order {
    return std.math.order(a.deadline_us, b.deadline_us);
}

pub const TimeoutQueue = std.PriorityQueue(TimeoutEntry, void, timeoutCompare);

fn calculateDeadlineUs(timeout_ms: u64) i64 {
    const now_us: i64 = std.time.microTimestamp();
    const delta_us: i64 = @intCast(timeout_ms * @as(u64, std.time.us_per_ms));
    return now_us + delta_us;
}

fn calculateDeadlineFromSecs(timeout_secs: f64) i64 {
    const now_us: i64 = std.time.microTimestamp();
    const delta_us: i64 = @intFromFloat(timeout_secs * @as(f64, @floatFromInt(std.time.us_per_s)));
    return now_us + delta_us;
}

const StreamWaitList = std.DoublyLinkedList(u64);

pub const BlockingManager = struct {
    allocator: std.mem.Allocator,
    blocked_clients_by_key: std.StringHashMap(std.DoublyLinkedList(u64)),
    blocked_stream_clients_by_key: std.StringHashMap(StreamWaitList),
    blocked_clients: std.AutoHashMap(u64, BlockedState),
    timeouts: TimeoutQueue,

    pub fn init(allocator: std.mem.Allocator) BlockingManager {
        return .{
            .allocator = allocator,
            .blocked_clients_by_key = std.StringHashMap(std.DoublyLinkedList(u64)).init(allocator),
            .blocked_stream_clients_by_key = std.StringHashMap(StreamWaitList).init(allocator),
            .blocked_clients = std.AutoHashMap(u64, BlockedState).init(allocator),
            .timeouts = TimeoutQueue.init(allocator, {}),
        };
    }

    pub fn deinit(self: *BlockingManager) void {
        var it = self.blocked_clients_by_key.iterator();
        while (it.next()) |entry| {
            while (entry.value_ptr.popFirst()) |node| {
                self.allocator.destroy(node);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.blocked_clients_by_key.deinit();

        var stream_it = self.blocked_stream_clients_by_key.iterator();
        while (stream_it.next()) |entry| {
            while (entry.value_ptr.popFirst()) |node| {
                self.allocator.destroy(node);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.blocked_stream_clients_by_key.deinit();

        self.blocked_clients.deinit();
        self.timeouts.deinit();
    }

    pub fn getNextTimeoutMs(self: *BlockingManager, now_us: i64) c_int {
        if (self.timeouts.peek()) |t| {
            if (t.deadline_us <= now_us) return 0;
            const delta_us: i64 = t.deadline_us - now_us;
            const delta_ms_i64: i64 = @divTrunc(delta_us, @as(i64, @intCast(std.time.us_per_ms)));
            const max_cint: c_int = std.math.maxInt(c_int);
            if (delta_ms_i64 > max_cint) return max_cint;
            if (delta_ms_i64 < 0) return 0;
            return @intCast(delta_ms_i64);
        }
        return -1;
    }

    pub fn setDeadline(self: *BlockingManager, connection_id: u64, timeout_ms: ?u64) !void {
        if (timeout_ms) |ms| {
            if (ms == 0) return;
            const deadline_us = calculateDeadlineUs(ms);
            try self.timeouts.add(.{ .deadline_us = deadline_us, .connection_id = connection_id });
        }
    }

    pub fn getOrCreateBlockedList(self: *BlockingManager, key: []const u8) !struct { key: []const u8, wait_list: *std.DoublyLinkedList(u64) } {
        const gop = try self.blocked_clients_by_key.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = .{};
        }
        return .{ .key = gop.key_ptr.*, .wait_list = gop.value_ptr };
    }

    pub fn registerBlockedClient(self: *BlockingManager, connection_id: u64, key: []const u8) !struct { key: []const u8, node: *std.DoublyLinkedList(u64).Node } {
        const wait = try self.getOrCreateBlockedList(key);

        const NodeType = std.DoublyLinkedList(u64).Node;
        const node_ptr = try self.allocator.create(NodeType);
        node_ptr.* = .{ .data = connection_id };
        wait.wait_list.append(node_ptr);

        return .{ .key = wait.key, .node = node_ptr };
    }

    pub fn removeBlockedClientNode(self: *BlockingManager, key: []const u8, node_ptr: *std.DoublyLinkedList(u64).Node) void {
        if (self.blocked_clients_by_key.getPtr(key)) |wait_list| {
            wait_list.remove(node_ptr);
        }
        self.allocator.destroy(node_ptr);
    }

    pub fn getOrCreateStreamWaitList(self: *BlockingManager, key: []const u8) !struct { key: []const u8, wait_list: *StreamWaitList } {
        const gop = try self.blocked_stream_clients_by_key.getOrPut(key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = .{};
        }
        return .{ .key = gop.key_ptr.*, .wait_list = gop.value_ptr };
    }

    fn cleanupStreamSlices(self: *BlockingManager, requests: []StreamReadRequest, registrations: []StreamRegistration) void {
        for (registrations) |registration| {
            if (self.blocked_stream_clients_by_key.getPtr(registration.key)) |wait_list| {
                wait_list.remove(registration.node);
            }
            self.allocator.destroy(registration.node);
        }

        for (requests) |request| {
            self.allocator.free(request.id);
        }
    }

    pub fn createStreamBlockingState(self: *BlockingManager, connection_id: u64, requests: []const StreamReadRequest) !StreamBlockingState {
        const count = requests.len;
        const stored_requests = try self.allocator.alloc(StreamReadRequest, count);
        errdefer self.allocator.free(stored_requests);

        const registrations = try self.allocator.alloc(StreamRegistration, count);
        errdefer self.allocator.free(registrations);

        var created: usize = 0;
        errdefer self.cleanupStreamSlices(stored_requests[0..created], registrations[0..created]);

        for (requests, 0..) |request, idx| {
            const wait = try self.getOrCreateStreamWaitList(request.key);
            const node_ptr = try self.allocator.create(StreamWaitList.Node);
            node_ptr.* = .{ .data = connection_id };
            wait.wait_list.append(node_ptr);

            stored_requests[idx] = .{
                .key = wait.key,
                .id = try self.allocator.dupe(u8, request.id),
            };
            registrations[idx] = .{ .key = wait.key, .node = node_ptr };
            created = idx + 1;
        }

        return .{ .requests = stored_requests, .registrations = registrations };
    }

    pub fn cleanupStreamBlockingState(self: *BlockingManager, maybe_state: ?StreamBlockingState) void {
        if (maybe_state) |state| {
            for (state.registrations) |registration| {
                if (self.blocked_stream_clients_by_key.getPtr(registration.key)) |wait_list| {
                    wait_list.remove(registration.node);
                }
                self.allocator.destroy(registration.node);
            }

            for (state.requests) |request| {
                self.allocator.free(request.id);
            }

            self.allocator.free(state.registrations);
            self.allocator.free(state.requests);
        }
    }

    pub fn clearStreamState(self: *BlockingManager, connection_id: u64) void {
        if (self.blocked_clients.get(connection_id)) |state| {
            switch (state) {
                .Stream => |s| self.cleanupStreamBlockingState(s),
                else => {},
            }
        }
        _ = self.blocked_clients.remove(connection_id);
    }

    pub fn registerStreamBlockingClient(
        self: *BlockingManager,
        connection_id: u64,
        requests: []const StreamReadRequest,
        block_ms: ?u64,
    ) !void {
        const state = try self.createStreamBlockingState(connection_id, requests);
        errdefer self.cleanupStreamBlockingState(state);

        try self.blocked_clients.put(connection_id, .{ .Stream = state });
        try self.setDeadline(connection_id, block_ms);
    }

    pub fn registerListBlockingClient(
        self: *BlockingManager,
        connection_id: u64,
        key: []const u8,
        timeout_secs: f64,
    ) !void {
        const reg = try self.registerBlockedClient(connection_id, key);
        try self.blocked_clients.put(connection_id, .{ .List = .{ .key = reg.key, .node = reg.node } });
        if (timeout_secs > 0) {
            const deadline_us = calculateDeadlineFromSecs(timeout_secs);
            try self.timeouts.add(.{ .deadline_us = deadline_us, .connection_id = connection_id });
        }
    }

    pub fn registerWaitBlockingClient(
        self: *BlockingManager,
        connection_id: u64,
        wait_state: WaitState,
        timeout_ms: u64,
    ) !void {
        try self.blocked_clients.put(connection_id, .{ .Wait = wait_state });
        try self.setDeadline(connection_id, timeout_ms);
    }
};
