const std = @import("std");
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;
const EPOLL = linux.EPOLL;

const log = std.log.scoped(.server);

const MAX_EPOLL_EVENTS = 128;
const TCP_BACKLOG = 128;
const READ_BUFFER_SIZE = 262144;

pub fn Server(comptime H: type) type {
    return struct {
        const Self = @This();

        handler: *H,
        epoll_fd: posix.fd_t,
        listener_fd: posix.fd_t,
        quit_fd: posix.fd_t,

        pub fn init(handler: *H, host: []const u8, port: u16) !Server(H) {
            var sigmask = posix.empty_sigset;
            linux.sigaddset(&sigmask, posix.SIG.INT);
            linux.sigaddset(&sigmask, posix.SIG.TERM);
            posix.sigprocmask(posix.SIG.BLOCK, &sigmask, null);

            const quit_fd = try posix.signalfd(-1, &sigmask, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK);
            errdefer posix.close(quit_fd);

            const address = try net.Address.resolveIp(host, port);

            const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
            const protocol = posix.IPPROTO.TCP;
            const listener_fd = try posix.socket(address.any.family, socket_type, protocol);

            try posix.setsockopt(listener_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
            try posix.bind(listener_fd, &address.any, address.getOsSockLen());
            try posix.listen(listener_fd, 128);

            const epoll_fd = try posix.epoll_create1(0);
            errdefer posix.close(epoll_fd);

            try addFdToEpoll(epoll_fd, listener_fd, EPOLL.IN);
            try addFdToEpoll(epoll_fd, quit_fd, EPOLL.IN);
            log.info("Server listening on {s}:{d}", .{ host, port });

            return .{ .handler = handler, .epoll_fd = epoll_fd, .listener_fd = listener_fd, .quit_fd = quit_fd };
        }

        pub fn deinit(self: *Self) void {
            posix.close(self.listener_fd);
            posix.close(self.quit_fd);
            posix.close(self.epoll_fd);
            log.info("Server shut down gracefully.", .{});
        }

        fn handleClient(self: *Self, client_fd: posix.fd_t, events: u32) !void {
            if ((events & (EPOLL.HUP | EPOLL.ERR)) != 0) return self.closeConnection(client_fd);

            var client_file = std.fs.File{ .handle = client_fd };
            var buffer: [READ_BUFFER_SIZE]u8 = undefined;

            const bytes_read = client_file.read(&buffer) catch {
                return self.closeConnection(client_fd);
            };

            if (bytes_read == 0) return self.closeConnection(client_fd);

            try self.handler.handleRequest(client_fd, buffer[0..bytes_read]);
        }

        pub fn run(self: *Self) !void {
            var ready_list: [MAX_EPOLL_EVENTS]linux.epoll_event = undefined;

            main_loop: while (true) {
                const num_events = posix.epoll_wait(self.epoll_fd, &ready_list, -1);

                for (ready_list[0..num_events]) |event| {
                    const fd = event.data.fd;

                    if (fd == self.quit_fd) {
                        break :main_loop;
                    } else if (fd == self.listener_fd) {
                        try self.acceptConnection();
                    } else {
                        try self.handleClient(fd, event.events);
                    }
                }
            }
        }

        fn acceptConnection(self: *Self) !void {
            const client_fd = try posix.accept(self.listener_fd, null, null, posix.SOCK.NONBLOCK);
            errdefer posix.close(client_fd);

            try addFdToEpoll(self.epoll_fd, client_fd, linux.EPOLL.IN);
            try self.handler.addConnection(client_fd);

            log.info("Accepted connection: fd={d}", .{client_fd});
        }

        fn closeConnection(self: *Self, client_fd: posix.fd_t) void {
            self.handler.removeConnection(client_fd);
            posix.close(client_fd);
            log.info("Closed connection: fd={d}", .{client_fd});
        }
    };
}

fn addFdToEpoll(epoll_fd: posix.fd_t, fd: posix.fd_t, events: u32) !void {
    var event = linux.epoll_event{
        .events = events,
        .data = .{ .fd = fd },
    };
    try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, fd, &event);
}
