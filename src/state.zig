const std = @import("std");
const db = @import("data_structures/mod.zig");
const dispatcher = @import("dispatcher.zig");
const posix = std.posix;
const format = @import("resp/format.zig");
const Reply = @import("reply.zig").Reply;
const Order = std.math.Order;

pub const Notify = struct {
    fd: std.posix.fd_t,
    bytes: []const u8,
};

pub const ClientConnection = struct {
    fd: posix.fd_t,
    blocking_key: ?[]const u8,
    blocking_node: ?*std.DoublyLinkedList(posix.fd_t).Node,
    deadline_us: ?i64,
};

pub const AppHandler = struct {
    app_allocator: std.mem.Allocator,
    string_store: *db.StringStore,
    list_store: *db.ListStore,

    connection_by_fd: std.hash_map.AutoHashMap(posix.fd_t, ClientConnection),
    blocked_clients_by_key: std.hash_map.StringHashMap(std.DoublyLinkedList(posix.fd_t)),

    timeouts: TimeoutQueue,

    const TimeoutEntry = struct {
        deadline_us: i64,
        fd: posix.fd_t,
    };

    fn timeoutCompare(_: void, a: TimeoutEntry, b: TimeoutEntry) Order {
        return std.math.order(a.deadline_us, b.deadline_us);
    }

    const TimeoutQueue = std.PriorityQueue(TimeoutEntry, void, timeoutCompare);

    pub fn init(allocator: std.mem.Allocator, string_store: *db.StringStore, list_store: *db.ListStore) AppHandler {
        const connection_by_fd = std.hash_map.AutoHashMap(posix.fd_t, ClientConnection).init(allocator);
        const blocked_clients_by_key = std.hash_map.StringHashMap(std.DoublyLinkedList(posix.fd_t)).init(allocator);
        const timeouts = TimeoutQueue.init(allocator, {});

        return .{
            .app_allocator = allocator,
            .string_store = string_store,
            .list_store = list_store,
            .connection_by_fd = connection_by_fd,
            .blocked_clients_by_key = blocked_clients_by_key,
            .timeouts = timeouts,
        };
    }

    pub fn deinit(self: *AppHandler) void {
        var it = self.blocked_clients_by_key.iterator();
        while (it.next()) |entry| {
            self.app_allocator.free(entry.key_ptr.*);
        }
        self.blocked_clients_by_key.deinit();

        self.connection_by_fd.deinit();
        self.timeouts.deinit();
    }

    pub fn handleRequest(self: *AppHandler, client_fd: posix.fd_t, request_data: []const u8) !void {
        var arena = std.heap.ArenaAllocator.init(self.app_allocator);
        defer arena.deinit();

        const request_allocator = arena.allocator();

        const client_connection = self.connection_by_fd.getPtr(client_fd).?;
        const response = try dispatcher.processRequest(self, request_allocator, request_data, client_connection);

        switch (response) {
            .Immediate => |value| {
                var client_file = std.fs.File{ .handle = client_fd };
                client_file.writer().writeAll(value.bytes) catch self.removeConnection(client_fd);

                if (value.notify) |ns| {
                    self.emitNotifications(ns);
                }
            },
            .Blocked => {},
        }
    }

    pub fn emitNotifications(self: *AppHandler, notifications: []Notify) void {
        for (notifications) |n| {
            var out_file = std.fs.File{ .handle = n.fd };
            out_file.writer().writeAll(n.bytes) catch {
                self.removeConnection(n.fd);
            };
        }
    }

    pub fn addConnection(self: *AppHandler, client_fd: posix.fd_t) !void {
        try self.connection_by_fd.put(client_fd, ClientConnection{
            .fd = client_fd,
            .blocking_key = null,
            .blocking_node = null,
            .deadline_us = null,
        });
    }

    pub fn removeConnection(self: *AppHandler, client_fd: posix.fd_t) void {
        if (self.connection_by_fd.fetchRemove(client_fd)) |removed_connection| {
            if (removed_connection.value.blocking_key) |key| {
                const wait_list = self.blocked_clients_by_key.getPtr(key).?;
                const node_ptr = removed_connection.value.blocking_node.?;
                wait_list.remove(node_ptr);
                self.app_allocator.destroy(node_ptr);
            }
        }
    }

    pub fn drainWaitersForKey(self: *AppHandler, request_allocator: std.mem.Allocator, key: []const u8, pushed: usize) ![]Notify {
        var notifications = std.ArrayList(Notify).init(request_allocator);

        const wait_list_ptr = self.blocked_clients_by_key.getPtr(key) orelse return &[_]Notify{};

        for (0..pushed) |_| {
            const popped_values = try self.list_store.lpop(key, 1, request_allocator);

            const node_ptr = wait_list_ptr.popFirst() orelse break;
            const fd = node_ptr.data;
            defer self.app_allocator.destroy(node_ptr);

            if (self.connection_by_fd.getPtr(fd)) |conn| {
                conn.blocking_key = null;
                conn.blocking_node = null;
                conn.deadline_us = null;
            }

            var buf = std.ArrayList(u8).init(request_allocator);
            var arr = try request_allocator.alloc(Reply, 2);
            arr[0] = Reply{ .BulkString = key };
            arr[1] = Reply{ .BulkString = popped_values[0] };

            try format.writeReply(buf.writer(), Reply{ .Array = arr });
            const bytes = try buf.toOwnedSlice();
            try notifications.append(.{ .fd = fd, .bytes = bytes });
        }

        return try notifications.toOwnedSlice();
    }

    pub fn getNextTimeoutMs(self: *AppHandler, now_us: i64) c_int {
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

    pub fn expireDueWaiters(self: *AppHandler, now_us: i64) void {
        while (self.timeouts.peek()) |t| {
            if (t.deadline_us > now_us) break;

            const entry = self.timeouts.remove();
            const fd = entry.fd;

            if (self.connection_by_fd.getPtr(fd)) |conn| {
                const maybe_key = conn.blocking_key;
                const maybe_node = conn.blocking_node;
                if (maybe_key) |key| {
                    if (maybe_node) |node_ptr| {
                        if (self.blocked_clients_by_key.getPtr(key)) |wait_list| {
                            wait_list.remove(node_ptr);
                        }
                        self.app_allocator.destroy(node_ptr);
                    }

                    conn.blocking_key = null;
                    conn.blocking_node = null;
                    conn.deadline_us = null;

                    var out_file = std.fs.File{ .handle = fd };
                    format.writeReply(out_file.writer(), Reply{ .Array = null }) catch {
                        self.removeConnection(fd);
                    };
                }
            }
        }
    }
};
