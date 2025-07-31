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
    LRANGE,

    pub fn fromSlice(slice: []const u8) ?Command {
        if (std.ascii.eqlIgnoreCase(slice, "PING")) return .PING;
        if (std.ascii.eqlIgnoreCase(slice, "ECHO")) return .ECHO;
        if (std.ascii.eqlIgnoreCase(slice, "SET")) return .SET;
        if (std.ascii.eqlIgnoreCase(slice, "GET")) return .GET;
        if (std.ascii.eqlIgnoreCase(slice, "RPUSH")) return .RPUSH;
        if (std.ascii.eqlIgnoreCase(slice, "LRANGE")) return .LRANGE;
        return null;
    }
};

const NULL_STRING = "$-1\r\n";
const PONG_MESSAGE = "+PONG\r\n";
const OK_MESSAGE = "+OK\r\n";

const ERR_ARG_NUM = "ERR wrong number of arguments";

fn formatSimpleString(allocator: std.mem.Allocator, string: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "+{s}\r\n", .{string});
}

fn formatBulkString(allocator: std.mem.Allocator, string: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ string.len, string });
}

fn formatInteger(allocator: std.mem.Allocator, value: u64) ![]const u8 {
    return std.fmt.allocPrint(allocator, ":{}\r\n", .{value});
}

fn formatStringArray(allocator: std.mem.Allocator, strings: []const []const u8) ![]const u8 {
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    const writer = buffer.writer();
    try writer.print("*{d}\r\n", .{strings.len});
    for (strings) |s| {
        try writer.print("${d}\r\n{s}\r\n", .{ s.len, s });
    }

    return buffer.toOwnedSlice();
}

fn calculateExpiration(ttl_ms: u64) i64 {
    const current_micros = std.time.microTimestamp();
    const ttl_us: i64 = @intCast(ttl_ms * std.time.us_per_ms);
    return current_micros + ttl_us;
}

fn handleEcho(allocator: std.mem.Allocator, args: [64]?[]const u8) ![]const u8 {
    const content = args[1] orelse return allocator.dupe(u8, NULL_STRING);
    return try formatBulkString(allocator, content);
}

fn handleGet(allocator: std.mem.Allocator, store: *Store, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return formatSimpleString(allocator, ERR_ARG_NUM);

    if (store.get(key)) |value_data| {
        return formatBulkString(allocator, value_data);
    } else {
        return allocator.dupe(u8, NULL_STRING);
    }
}

fn handleSet(allocator: std.mem.Allocator, store: *Store, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return formatSimpleString(allocator, ERR_ARG_NUM);
    const value_slice = args[2] orelse return formatSimpleString(allocator, ERR_ARG_NUM);
    const px_command = args[3] orelse "";

    var expiration_us: ?i64 = null;
    if (std.ascii.eqlIgnoreCase(px_command, "PX")) {
        const px_value_slice = args[4] orelse return formatSimpleString(allocator, "ERR syntax error");
        const px_value_ms = std.fmt.parseInt(u64, px_value_slice, 10) catch return formatSimpleString(allocator, "ERR value is not an integer or out of range");
        expiration_us = calculateExpiration(px_value_ms);
    }

    try store.set(key, value_slice, expiration_us);
    return allocator.dupe(u8, OK_MESSAGE);
}

fn handleRpush(allocator: std.mem.Allocator, lists_store: *Lists, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return formatSimpleString(allocator, ERR_ARG_NUM);

    var length: u64 = 0;
    for (args[2..]) |value_opt| {
        const value = value_opt orelse break;
        length = try lists_store.append(key, value);
    }

    return formatInteger(allocator, length);
}

fn handleLrange(allocator: std.mem.Allocator, lists_store: *Lists, args: [64]?[]const u8) ![]const u8 {
    const key = args[1] orelse return formatSimpleString(allocator, ERR_ARG_NUM);
    const start_index_slice = args[2] orelse return formatSimpleString(allocator, ERR_ARG_NUM);
    const end_index_slice = args[3] orelse return formatSimpleString(allocator, ERR_ARG_NUM);

    const start_index = std.fmt.parseInt(i64, start_index_slice, 10) catch return formatSimpleString(allocator, "ERR value is not an integer or out of range");
    const end_index = std.fmt.parseInt(i64, end_index_slice, 10) catch return formatSimpleString(allocator, "ERR value is not an integer or out of range");

    const range = lists_store.lrange(key, start_index, end_index);
    return formatStringArray(allocator, range);
}

fn processRequest(handler: *AppHandler, allocator: std.mem.Allocator, request_data: []const u8) ![]const u8 {
    const commands = try resp.RESP.parse(request_data);

    var response_buffer = std.ArrayList(u8).init(allocator);

    for (commands) |command_parts_optional| {
        const command_parts = command_parts_optional orelse continue;
        if (command_parts.len == 0) continue;

        const command_str = command_parts[0] orelse continue;
        const command = Command.fromSlice(command_str) orelse continue;

        const response = try switch (command) {
            .PING => allocator.dupe(u8, PONG_MESSAGE),
            .ECHO => handleEcho(allocator, command_parts),
            .GET => handleGet(allocator, handler.store, command_parts),
            .SET => handleSet(allocator, handler.store, command_parts),
            .RPUSH => handleRpush(allocator, handler.lists, command_parts),
            .LRANGE => handleLrange(allocator, handler.lists, command_parts),
        };
        defer allocator.free(response);

        try response_buffer.writer().writeAll(response);
    }

    return response_buffer.toOwnedSlice();
}

const AppHandler = struct {
    allocator: std.mem.Allocator,
    store: *Store,
    lists: *Lists,

    pub fn init(allocator: std.mem.Allocator, store: *Store, lists_store: *Lists) AppHandler {
        return .{ .allocator = allocator, .store = store, .lists = lists_store };
    }

    pub fn handleRequest(self: *AppHandler, client_fd: posix.fd_t, request_data: []const u8) !void {
        const response = try processRequest(self, self.allocator, request_data);
        defer self.allocator.free(response);

        var client_file = std.fs.File{ .handle = client_fd };
        try client_file.writer().writeAll(response);
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

    const app_handler = AppHandler.init(allocator, &store, &lists_store);

    var server_instance = try server.Server(AppHandler).init(app_handler, "0.0.0.0", 6379);
    defer server_instance.deinit();

    try server_instance.run();
}
