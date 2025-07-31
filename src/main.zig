const std = @import("std");

const server = @import("server.zig");
const key_value_store = @import("data_structures/key_value_store.zig");
const lists = @import("data_structures/lists.zig");
const resp = @import("resp.zig");

const logger = @import("log.zig");
pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = logger.logFn,
};

const posix = std.posix;
const log = std.log.scoped(.app);

const Store = key_value_store.KeyValueStore;
const Lists = lists.Lists;
const Writer = std.fs.File.Writer;

const Command = enum {
    PING,
    ECHO,
    SET,
    GET,
    RPUSH,

    pub fn fromSlice(slice: []const u8) ?Command {
        if (std.ascii.eqlIgnoreCase(slice, "PING")) return .PING;
        if (std.ascii.eqlIgnoreCase(slice, "ECHO")) return .ECHO;
        if (std.ascii.eqlIgnoreCase(slice, "SET")) return .SET;
        if (std.ascii.eqlIgnoreCase(slice, "GET")) return .GET;
        if (std.ascii.eqlIgnoreCase(slice, "RPUSH")) return .RPUSH;
        return null;
    }
};

fn sendNullString(writer: std.fs.File.Writer) !void {
    try writer.writeAll("$-1\r\n");
}

fn sendBulkString(writer: std.fs.File.Writer, string: []const u8) !void {
    try writer.print("${d}\r\n{s}\r\n", .{ string.len, string });
}

fn sendSimpleString(writer: std.fs.File.Writer, string: []const u8) !void {
    try writer.print("+{s}\r\n", .{string});
}

fn sendInteger(writer: std.fs.File.Writer, value: u64) !void {
    try writer.print(":{}\r\n", .{value});
}

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}

fn handlePing(writer: Writer) !void {
    try sendSimpleString(writer, "PONG");
}

fn handleEcho(writer: Writer, args: [64]?[]const u8) !void {
    const content = args[1] orelse return;
    try sendBulkString(writer, content);
}

fn handleGet(store: *Store, writer: Writer, args: [64]?[]const u8) !void {
    const key = args[1] orelse return;

    if (store.get(key)) |value_data| {
        try sendBulkString(writer, value_data);
    } else {
        try sendNullString(writer);
    }
}

fn handleSet(store: *Store, writer: Writer, args: [64]?[]const u8) !void {
    const key = args[1] orelse return;
    const value_slice = args[2] orelse return;
    const px_command = args[3] orelse "";

    var expiration_us: ?i64 = null;
    if (std.ascii.eqlIgnoreCase(px_command, "PX")) {
        const px_value_slice = args[4] orelse return;
        const px_value_ms = std.fmt.parseInt(u64, px_value_slice, 10) catch return;
        expiration_us = calculateExpiration(px_value_ms);
    }

    try store.set(key, value_slice, expiration_us);
    try sendSimpleString(writer, "OK");
}

fn handleRpush(lists_store: *Lists, writer: Writer, args: [64]?[]const u8) !void {
    const key = args[1] orelse return;
    const value_slice = args[2] orelse return;

    const length = try lists_store.append(key, value_slice);
    try sendInteger(writer, length);
}

fn processRequest(handler: *AppHandler, writer: Writer, request_data: []const u8) !void {
    const commands = try resp.RESP.parse(request_data);

    for (commands) |command_parts_optional| {
        const command_parts = command_parts_optional orelse continue;
        if (command_parts.len == 0) continue;

        const command_str = command_parts[0] orelse continue;
        const command = Command.fromSlice(command_str) orelse continue;

        switch (command) {
            .PING => try handlePing(writer),
            .ECHO => try handleEcho(writer, command_parts),
            .GET => try handleGet(handler.store, writer, command_parts),
            .SET => try handleSet(handler.store, writer, command_parts),
            .RPUSH => try handleRpush(handler.lists, writer, command_parts),
        }
    }
}

const AppHandler = struct {
    store: *Store,
    lists: *Lists,

    pub fn init(store: *Store, lists_store: *Lists) AppHandler {
        return .{ .store = store, .lists = lists_store };
    }

    pub fn handleRequest(self: *AppHandler, client_fd: posix.fd_t, request_data: []const u8) !void {
        var client_file = std.fs.File{ .handle = client_fd };
        try processRequest(self, client_file.writer(), request_data);
    }
};

pub fn main() !void {
    log.info("Initializing server...", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var store = Store.init(allocator);
    defer store.deinit();

    var lists_store = Lists.init(allocator);
    defer lists_store.deinit();

    const app_handler = AppHandler.init(&store, &lists_store);

    var server_instance = try server.Server(AppHandler).init(app_handler, "0.0.0.0", 6379);
    defer server_instance.deinit();

    try server_instance.run();
}
