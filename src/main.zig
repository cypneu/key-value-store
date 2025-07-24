const std = @import("std");
const net = std.net;
const posix = std.posix;

pub fn main() !void {
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    const socket_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK;
    const protocol = posix.IPPROTO.TCP;
    const listener = try posix.socket(address.any.family, socket_type, protocol);
    defer posix.close(listener);

    try posix.setsockopt(listener, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try posix.bind(listener, &address.any, address.getOsSockLen());
    try posix.listen(listener, 128);

    const stdout = std.io.getStdOut().writer();
    try stdout.print("Server listening on 127.0.0.1:6379\n", .{});

    const kfd = try posix.kqueue();
    defer posix.close(kfd);

    // while (true) {
    // const stream = std.net.Stream{ .handle = socket };
    // var buffer: [1024]u8 = undefined;
    // const read = stream.read(&buffer) catch {
    //     // std.debug.print("error read: {}\n", .{err});
    //     continue;
    // };
    // if (read == 0) {
    //     continue;
    // }
    // std.debug.print("Yo", .{});
    // try stream.writeAll("+PONG\r\n");
    // //     const connection = try listener.accept();
    // //     defer connection.stream.close();
    // //
    // //     var buffer: [1024]u8 = undefined;
    // //     while (try connection.stream.reader().read(&buffer) > 0) {
    // //         try connection.stream.writeAll("+PONG\r\n");
    // //     }
    // }
}
