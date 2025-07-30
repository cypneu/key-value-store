const std = @import("std");
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;

const resp = @import("resp.zig");

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}

fn isExpired(expiration_us: i64) bool {
    return std.time.microTimestamp() >= expiration_us;
}

const Value = struct {
    data: []const u8,
    expiration_us: ?i64,
};

fn sendNotFound(writer: anytype) !void {
    _ = try writer.write("$-1\r\n");
}

fn sendValue(writer: anytype, v: Value) !void {
    try writer.print("${d}\r\n{s}\r\n", .{ v.data.len, v.data });
}

pub fn main() !void {
    var sigmask = posix.empty_sigset;
    linux.sigaddset(&sigmask, posix.SIG.INT);
    linux.sigaddset(&sigmask, posix.SIG.TERM);
    posix.sigprocmask(posix.SIG.BLOCK, &sigmask, null);

    const quit_fd = try posix.signalfd(-1, &sigmask, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK);
    defer posix.close(quit_fd);

    const address = try net.Address.resolveIp("0.0.0.0", 6379);

    const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
    const protocol = posix.IPPROTO.TCP;
    const listener = try posix.socket(address.any.family, socket_type, protocol);
    defer posix.close(listener);

    try posix.setsockopt(listener, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try posix.bind(listener, &address.any, address.getOsSockLen());
    try posix.listen(listener, 128);

    const stdout = std.io.getStdOut().writer();
    try stdout.print("Server listening on 0.0.0.0:6379\n", .{});

    const efd = try posix.epoll_create1(0);
    defer posix.close(efd);

    {
        var event = linux.epoll_event{ .events = linux.EPOLL.IN, .data = .{ .fd = listener } };
        try posix.epoll_ctl(efd, linux.EPOLL.CTL_ADD, listener, &event);
    }
    {
        var event = linux.epoll_event{ .events = linux.EPOLL.IN, .data = .{ .fd = quit_fd } };
        try posix.epoll_ctl(efd, linux.EPOLL.CTL_ADD, quit_fd, &event);
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var map = std.StringHashMap(Value).init(allocator);
    defer {
        {
            var it = map.iterator();
            while (it.next()) |e| allocator.free(e.value_ptr.data);
        }
        map.deinit();
    }

    var ready_list: [128]linux.epoll_event = undefined;
    main_loop: while (true) {
        const ready_count = posix.epoll_wait(efd, &ready_list, -1);

        for (ready_list[0..ready_count]) |ready| {
            const ready_socket = ready.data.fd;
            if (ready_socket == quit_fd) {
                std.debug.print("\nSignal received, shutting down gracefully.\n", .{});
                break :main_loop;
            }

            if (ready_socket == listener) {
                const client_socket = try posix.accept(listener, null, null, posix.SOCK.NONBLOCK);
                errdefer posix.close(client_socket);
                var event = linux.epoll_event{ .events = linux.EPOLL.IN, .data = .{ .fd = client_socket } };
                try posix.epoll_ctl(efd, linux.EPOLL.CTL_ADD, client_socket, &event);
            } else {
                var client_file = std.fs.File{ .handle = ready_socket };
                var buf_reader = std.io.bufferedReader(client_file.reader());
                const reader = buf_reader.reader();
                const writer = client_file.writer();

                var buffer: [1024]u8 = undefined;
                const read = reader.read(&buffer) catch 0;
                if (read == 0 or ready.events & linux.EPOLL.RDHUP == linux.EPOLL.RDHUP) {
                    posix.close(ready_socket);
                    continue;
                }

                const commands_array = try resp.RESP.parse(&buffer);
                for (commands_array) |command_array_optional| {
                    const actual_command_array = command_array_optional orelse continue;

                    if (actual_command_array[0]) |command| {
                        if (std.ascii.eqlIgnoreCase(command, "PING")) {
                            _ = try posix.write(ready_socket, "+PONG\r\n");
                        } else if (std.ascii.eqlIgnoreCase(command, "ECHO")) {
                            if (actual_command_array[1]) |content| {
                                _ = try writer.print("${d}\r\n{s}\r\n", .{ content.len, content });
                            }
                        } else if (std.ascii.eqlIgnoreCase(command, "SET")) {
                            if (actual_command_array[1]) |key| {
                                if (actual_command_array[2]) |value_slice| {
                                    const owned_value = try allocator.dupe(u8, value_slice);
                                    errdefer allocator.free(owned_value);

                                    var value = Value{ .data = owned_value, .expiration_us = null };

                                    if (actual_command_array[3]) |px| {
                                        if (std.ascii.eqlIgnoreCase(px, "PX")) {
                                            if (actual_command_array[4]) |px_value_slice| {
                                                if (std.fmt.parseInt(u64, px_value_slice, 10)) |px_value_ms| {
                                                    value.expiration_us = calculateExpiration(px_value_ms);
                                                } else |err| {
                                                    std.debug.print("Error: {}\n", .{err});
                                                    continue;
                                                }
                                            }
                                        }
                                    }

                                    if (try map.fetchPut(key, value)) |kv| {
                                        allocator.free(kv.value.data);
                                    }

                                    _ = try posix.write(ready_socket, "+OK\r\n");
                                }
                            }
                        } else if (std.ascii.eqlIgnoreCase(command, "GET")) {
                            const key = actual_command_array[1] orelse continue;
                            const value = map.get(key) orelse {
                                try sendNotFound(writer);
                                continue;
                            };
                            const is_key_expired = if (value.expiration_us) |exp| isExpired(exp) else false;

                            if (is_key_expired) {
                                if (map.fetchRemove(key)) |removed| {
                                    allocator.free(removed.value.data);
                                }
                                try sendNotFound(writer);
                            } else try sendValue(writer, value);
                        }
                    }
                }
            }
        }
    }
}
