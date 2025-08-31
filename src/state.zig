const std = @import("std");
const db = @import("data_structures/mod.zig");
const dispatcher = @import("dispatcher.zig");
const posix = std.posix;
const format = @import("resp/format.zig");
const Reply = @import("reply.zig").Reply;

pub const Notify = struct {
    fd: std.posix.fd_t,
    bytes: []const u8,
};

pub const ClientConnection = struct {
    fd: posix.fd_t,
    blocking_key: ?[]const u8,
    blocking_node: ?*std.DoublyLinkedList(posix.fd_t).Node,
    // TODO: Add input and outbuf buffer per client
};

pub const AppHandler = struct {
    app_allocator: std.mem.Allocator,
    string_store: *db.StringStore,
    list_store: *db.ListStore,

    connection_by_fd: std.hash_map.AutoHashMap(posix.fd_t, ClientConnection),
    blocked_clients_by_key: std.hash_map.StringHashMap(std.DoublyLinkedList(posix.fd_t)),

    pub fn init(allocator: std.mem.Allocator, string_store: *db.StringStore, list_store: *db.ListStore) AppHandler {
        const connection_by_fd = std.hash_map.AutoHashMap(posix.fd_t, ClientConnection).init(allocator);
        const blocked_clients_by_key = std.hash_map.StringHashMap(std.DoublyLinkedList(posix.fd_t)).init(allocator);

        return .{
            .app_allocator = allocator,
            .string_store = string_store,
            .list_store = list_store,
            .connection_by_fd = connection_by_fd,
            .blocked_clients_by_key = blocked_clients_by_key,
        };
    }

    pub fn deinit(self: *AppHandler) void {
        var it = self.blocked_clients_by_key.iterator();
        while (it.next()) |entry| {
            self.app_allocator.free(entry.key_ptr.*);
        }
        self.blocked_clients_by_key.deinit();

        self.connection_by_fd.deinit();
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
};
