const std = @import("std");
const server = @import("server.zig");
const db = @import("data_structures/mod.zig");
const logger = @import("log.zig");
const Command = @import("command.zig").Command;
const AppHandler = @import("state.zig").AppHandler;

const Writer = std.fs.File.Writer;

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = logger.logFn,
};

const log = std.log.scoped(.app);

pub fn main() !void {
    log.info("Initializing server...", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var string_store = db.StringStore.init(allocator);
    defer string_store.deinit();

    var list_store = db.ListStore.init(allocator);
    defer list_store.deinit();

    var app_handler = AppHandler.init(allocator, &string_store, &list_store);
    defer app_handler.deinit();

    var server_instance = try server.Server(AppHandler).init(&app_handler, "0.0.0.0", 6379, allocator);
    defer server_instance.deinit();

    try server_instance.run();
}
