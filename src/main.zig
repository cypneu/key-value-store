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
    defer config.deinit(allocator);

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

const Config = struct {
    port: u16,
    role: ServerRole,
    replica: ?ReplicaConfig,
    dir: []const u8,
    db_filename: []const u8,

    pub fn deinit(self: Config, allocator: std.mem.Allocator) void {
        allocator.free(self.dir);
        allocator.free(self.db_filename);
        if (self.replica) |r| allocator.free(r.host);
    }
};

const ConfigBuilder = struct {
    allocator: std.mem.Allocator,
    port: u16 = DEFAULT_PORT,
    role: ServerRole = .master,
    replica: ?ReplicaConfig = null,
    dir: ?[]const u8 = null,
    db_filename: ?[]const u8 = null,

    pub fn init(allocator: std.mem.Allocator) ConfigBuilder {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ConfigBuilder) void {
        if (self.replica) |r| self.allocator.free(r.host);
        if (self.dir) |d| self.allocator.free(d);
        if (self.db_filename) |f| self.allocator.free(f);
    }

    pub fn setPort(self: *ConfigBuilder, val: []const u8) !void {
        self.port = try std.fmt.parseInt(u16, val, 10);
    }

    pub fn setReplica(self: *ConfigBuilder, val: []const u8) !void {
        var tokens = std.mem.tokenizeScalar(u8, val, ' ');
        const host = tokens.next() orelse return error.InvalidReplicaOfValue;
        const port_str = tokens.next() orelse return error.InvalidReplicaOfValue;
        if (tokens.next() != null) return error.InvalidReplicaOfValue;

        const port = try std.fmt.parseInt(u16, port_str, 10);
        const host_copy = try self.allocator.dupe(u8, host);

        if (self.replica) |r| self.allocator.free(r.host);
        self.role = .replica;
        self.replica = .{ .host = host_copy, .port = port };
    }

    pub fn setDir(self: *ConfigBuilder, val: []const u8) !void {
        if (self.dir) |d| self.allocator.free(d);
        self.dir = try self.allocator.dupe(u8, val);
    }

    pub fn setDbFilename(self: *ConfigBuilder, val: []const u8) !void {
        if (self.db_filename) |f| self.allocator.free(f);
        self.db_filename = try self.allocator.dupe(u8, val);
    }

    pub fn build(self: *ConfigBuilder) !Config {
        const r_replica = self.replica;
        self.replica = null;
        errdefer if (r_replica) |r| self.allocator.free(r.host);

        const r_dir = if (self.dir) |d| d else try self.allocator.dupe(u8, ".");
        self.dir = null;
        errdefer self.allocator.free(r_dir);

        const r_db_filename = if (self.db_filename) |f| f else try self.allocator.dupe(u8, "dump.rdb");
        self.db_filename = null;
        errdefer self.allocator.free(r_db_filename);

        return Config{
            .port = self.port,
            .role = self.role,
            .replica = r_replica,
            .dir = r_dir,
            .db_filename = r_db_filename,
        };
    }
};

fn parseConfig(allocator: std.mem.Allocator) !Config {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();

    var builder = ConfigBuilder.init(allocator);
    defer builder.deinit();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            const value = args.next() orelse return error.MissingPortValue;
            try builder.setPort(value);
        } else if (std.mem.eql(u8, arg, "--replicaof")) {
            const value = args.next() orelse return error.MissingReplicaOfValue;
            try builder.setReplica(value);
        } else if (std.mem.eql(u8, arg, "--dir")) {
            const value = args.next() orelse return error.MissingDirValue;
            try builder.setDir(value);
        } else if (std.mem.eql(u8, arg, "--dbfilename")) {
            const value = args.next() orelse return error.MissingDbFilenameValue;
            try builder.setDbFilename(value);
        }
    }

    return builder.build();
}
