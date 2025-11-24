const std = @import("std");
const server = @import("server.zig");
const db = @import("data_structures/mod.zig");
const logger = @import("log.zig");
const Command = @import("command.zig").Command;
const state = @import("state.zig");
const AppHandler = state.AppHandler;
const ServerRole = state.ServerRole;
const ReplicaConfig = state.ReplicaConfig;

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

    const config = parseConfig(allocator) catch |err| {
        log.err("Failed to parse CLI args: {}", .{err});
        return err;
    };

    log.info("Initializing server on port {d}...", .{config.port});

    var string_store = db.StringStore.init(allocator);
    defer string_store.deinit();

    var list_store = db.ListStore.init(allocator);
    defer list_store.deinit();

    var stream_store = db.StreamStore.init(allocator);
    defer stream_store.deinit();

    var app_handler = AppHandler.init(allocator, &string_store, &list_store, &stream_store, config.role, config.replica, config.dir, config.db_filename);
    defer app_handler.deinit();

    const rdb = @import("rdb.zig");
    rdb.loadRDB(allocator, config.dir, config.db_filename, &app_handler) catch |err| {
        log.warn("Failed to load RDB file: {}", .{err});
    };

    var server_instance = try server.Server(AppHandler).init(&app_handler, "0.0.0.0", config.port, allocator);
    defer server_instance.deinit();

    if (config.role == .replica) {
        const master = config.replica.?;
        server_instance.connectToMaster(master.host, master.port, config.port) catch |err| {
            log.err("Failed to connect to master: {}", .{err});
        };
    }

    try server_instance.run();
}

const Config = struct { port: u16, role: ServerRole, replica: ?ReplicaConfig, dir: []const u8, db_filename: []const u8 };

fn parseConfig(allocator: std.mem.Allocator) !Config {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();

    var port: u16 = DEFAULT_PORT;
    var role: ServerRole = .master;
    var replica: ?ReplicaConfig = null;
    var dir: ?[]const u8 = null;
    var db_filename: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            const value = args.next() orelse return error.MissingPortValue;
            port = try std.fmt.parseInt(u16, value, 10);
        } else if (std.mem.eql(u8, arg, "--replicaof")) {
            const value = args.next() orelse return error.MissingReplicaOfValue;
            var tokens = std.mem.tokenizeScalar(u8, value, ' ');
            const host = tokens.next() orelse return error.InvalidReplicaOfValue;
            const port_str = tokens.next() orelse return error.InvalidReplicaOfValue;
            if (tokens.next() != null) return error.InvalidReplicaOfValue;

            const replica_port = try std.fmt.parseInt(u16, port_str, 10);
            const host_copy = try allocator.dupe(u8, host);

            role = .replica;
            replica = .{ .host = host_copy, .port = replica_port };
        } else if (std.mem.eql(u8, arg, "--dir")) {
            const value = args.next() orelse return error.MissingDirValue;
            dir = try allocator.dupe(u8, value);
        } else if (std.mem.eql(u8, arg, "--dbfilename")) {
            const value = args.next() orelse return error.MissingDbFilenameValue;
            db_filename = try allocator.dupe(u8, value);
        }
    }

    return .{
        .port = port,
        .role = role,
        .replica = replica,
        .dir = dir orelse try allocator.dupe(u8, "."),
        .db_filename = db_filename orelse try allocator.dupe(u8, "dump.rdb"),
    };
}
