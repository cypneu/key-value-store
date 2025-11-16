const std = @import("std");
const server = @import("server.zig");
const db = @import("data_structures/mod.zig");
const logger = @import("log.zig");
const Command = @import("command.zig").Command;
const AppHandler = @import("state.zig").AppHandler;

const Writer = std.fs.File.Writer;
const DEFAULT_PORT: u16 = 6379;

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = logger.logFn,
};

const log = std.log.scoped(.app);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const port = parsePort(allocator) catch |err| {
        log.err("Failed to parse CLI args: {}", .{err});
        return err;
    };

    log.info("Initializing server on port {d}...", .{port});

    var string_store = db.StringStore.init(allocator);
    defer string_store.deinit();

    var list_store = db.ListStore.init(allocator);
    defer list_store.deinit();

    var stream_store = db.StreamStore.init(allocator);
    defer stream_store.deinit();

    var app_handler = AppHandler.init(allocator, &string_store, &list_store, &stream_store);
    defer app_handler.deinit();

    var server_instance = try server.Server(AppHandler).init(&app_handler, "0.0.0.0", port, allocator);
    defer server_instance.deinit();

    try server_instance.run();
}

fn parsePort(allocator: std.mem.Allocator) !u16 {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();

    var port: u16 = DEFAULT_PORT;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            const value = args.next() orelse return error.MissingPortValue;
            port = try std.fmt.parseInt(u16, value, 10);
        }
    }

    return port;
}
