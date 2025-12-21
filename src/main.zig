const std = @import("std");
const Config = @import("config.zig").Config;
const Server = @import("network/server.zig").Server;
const logger = @import("log.zig");
const rdb_loader = @import("storage/rdb/loader.zig");

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = logger.logFn,
};

const log = std.log.scoped(.app);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        if (gpa.deinit() == .leak) {
            log.err("Memory leak detected!", .{});
            std.process.exit(1);
        }
    }
    const allocator = gpa.allocator();

    var config = Config.parse(allocator) catch |err| {
        log.err("Failed to parse CLI args: {}", .{err});
        return err;
    };
    defer config.deinit();

    log.info("Initializing server on port {d}...", .{config.port});

    var server = try Server.init(config);
    defer server.deinit();

    rdb_loader.loadRDB(config.allocator, config.dir, config.db_filename, &server.services) catch |err| {
        log.warn("Failed to load RDB file: {}", .{err});
    };

    if (config.role == .replica) {
        const master = config.replica.?;
        server.connectToMaster(master.host, master.port) catch |err| {
            log.err("Failed to connect to master: {}", .{err});
        };
    }

    try server.run();
}

test {
    _ = @import("storage/rdb/listpack.zig");
    _ = @import("storage/rdb/lzf.zig");
    _ = @import("storage/rdb/writer.zig");
}
