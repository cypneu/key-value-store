const std = @import("std");

const server = @import("server.zig");
const db = @import("data_structures/mod.zig");
const resp = @import("resp.zig");
const handlers = @import("handlers.zig");
const logger = @import("log.zig");

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = logger.logFn,
};

const posix = std.posix;
const log = std.log.scoped(.app);

const Writer = std.fs.File.Writer;

const Command = enum {
    PING,
    ECHO,
    SET,
    GET,
    RPUSH,
    LRANGE,
    LPUSH,
    LLEN,

    pub fn fromSlice(slice: []const u8) ?Command {
        if (std.ascii.eqlIgnoreCase(slice, "PING")) return .PING;
        if (std.ascii.eqlIgnoreCase(slice, "ECHO")) return .ECHO;
        if (std.ascii.eqlIgnoreCase(slice, "SET")) return .SET;
        if (std.ascii.eqlIgnoreCase(slice, "GET")) return .GET;
        if (std.ascii.eqlIgnoreCase(slice, "RPUSH")) return .RPUSH;
        if (std.ascii.eqlIgnoreCase(slice, "LRANGE")) return .LRANGE;
        if (std.ascii.eqlIgnoreCase(slice, "LPUSH")) return .LPUSH;
        if (std.ascii.eqlIgnoreCase(slice, "LLEN")) return .LLEN;
        return null;
    }
};

fn processRequest(handler: *AppHandler, request_allocator: std.mem.Allocator, request_data: []const u8) ![]const u8 {
    const commands = try resp.RESP.parse(request_data);

    var response_buffer = std.ArrayList(u8).init(request_allocator);

    for (commands) |command_parts_optional| {
        const command_parts = command_parts_optional orelse continue;
        if (command_parts.len == 0) continue;

        const command_str = command_parts[0] orelse continue;
        const command = Command.fromSlice(command_str) orelse continue;

        const response = try switch (command) {
            .PING => "+PONG\r\n",
            .ECHO => handlers.handleEcho(request_allocator, command_parts),
            .GET => handlers.handleGet(request_allocator, handler.string_store, command_parts),
            .SET => handlers.handleSet(request_allocator, handler.string_store, command_parts),
            .LPUSH => handlers.handleLpush(request_allocator, handler.list_store, command_parts),
            .RPUSH => handlers.handleRpush(request_allocator, handler.list_store, command_parts),
            .LRANGE => handlers.handleLrange(request_allocator, handler.list_store, command_parts),
            .LLEN => handlers.handleLlen(request_allocator, handler.list_store, command_parts),
        };

        try response_buffer.writer().writeAll(response);
    }

    return response_buffer.toOwnedSlice();
}

const AppHandler = struct {
    app_allocator: std.mem.Allocator,
    string_store: *db.StringStore,
    list_store: *db.ListStore,

    pub fn init(allocator: std.mem.Allocator, string_store: *db.StringStore, list_store: *db.ListStore) AppHandler {
        return .{ .app_allocator = allocator, .string_store = string_store, .list_store = list_store };
    }

    pub fn handleRequest(self: *AppHandler, client_fd: posix.fd_t, request_data: []const u8) !void {
        var arena = std.heap.ArenaAllocator.init(self.app_allocator);
        defer arena.deinit();

        const request_allocator = arena.allocator();

        const response = try processRequest(self, request_allocator, request_data);

        var client_file = std.fs.File{ .handle = client_fd };
        try client_file.writer().writeAll(response);
    }
};

pub fn main() !void {
    log.info("Initializing server...", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var string_store = db.StringStore.init(allocator);
    defer string_store.deinit();

    var list_store = db.ListStore.init(allocator);
    defer list_store.deinit();

    const app_handler = AppHandler.init(allocator, &string_store, &list_store);

    var server_instance = try server.Server(AppHandler).init(app_handler, "0.0.0.0", 6379);
    defer server_instance.deinit();

    try server_instance.run();
}
