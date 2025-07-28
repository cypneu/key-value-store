const std = @import("std");
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;

const resp = @import("resp.zig");

pub fn main() !void {
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

    var ready_list: [128]linux.epoll_event = undefined;
    while (true) {
        const ready_count = posix.epoll_wait(efd, &ready_list, -1);

        for (ready_list[0..ready_count]) |ready| {
            const ready_socket = ready.data.fd;

            if (ready_socket == listener) {
                const client_socket = try posix.accept(listener, null, null, posix.SOCK.NONBLOCK);
                errdefer posix.close(client_socket);
                var event = linux.epoll_event{ .events = linux.EPOLL.IN, .data = .{ .fd = client_socket } };
                try posix.epoll_ctl(efd, linux.EPOLL.CTL_ADD, client_socket, &event);
            } else {
                var closed = false;

                var client_file = std.fs.File{ .handle = ready_socket };
                var buf_reader = std.io.bufferedReader(client_file.reader());
                const reader = buf_reader.reader();

                var buffer: [1024]u8 = undefined;
                const read = reader.read(&buffer) catch 0;

                if (read == 0) {
                    closed = true;
                } else {
                    const input_array = try resp.RESP.parse(&buffer);
                    if (input_array[0]) |command| {
                        if (std.ascii.eqlIgnoreCase(command, "PING")) {
                            _ = try posix.write(ready_socket, "+PONG\r\n");
                        } else if (std.ascii.eqlIgnoreCase(command, "ECHO")) {
                            if (input_array[1]) |content| {
                                var writer = client_file.writer();
                                _ = try writer.print("${d}\r\n{s}\r\n", .{ content.len, content });
                            }
                        }
                    }
                }

                if (closed or ready.events & linux.EPOLL.RDHUP == linux.EPOLL.RDHUP) {
                    posix.close(ready_socket);
                }
            }
        }
    }
}
