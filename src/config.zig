const std = @import("std");
const replication = @import("features/replication.zig");

pub const ServerRole = replication.ServerRole;
pub const ReplicaConfig = replication.ReplicaConfig;

pub const Config = struct {
    allocator: std.mem.Allocator,
    host: []const u8 = "0.0.0.0",
    port: u16 = 6379,
    role: ServerRole = .master,
    replica: ?ReplicaConfig = null,
    dir: []const u8 = ".",
    db_filename: []const u8 = "dump.rdb",

    pub fn parse(allocator: std.mem.Allocator) !Config {
        var config = Config{ .allocator = allocator };

        var args = try std.process.argsWithAllocator(allocator);
        defer args.deinit();

        _ = args.next(); // Skip program name

        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--port")) {
                const value = args.next() orelse return error.MissingPortValue;
                config.port = try std.fmt.parseInt(u16, value, 10);
            } else if (std.mem.eql(u8, arg, "--replicaof")) {
                const value = args.next() orelse return error.MissingReplicaOfValue;
                try config.setReplica(value);
            } else if (std.mem.eql(u8, arg, "--dir")) {
                const value = args.next() orelse return error.MissingDirValue;
                config.dir = try allocator.dupe(u8, value);
            } else if (std.mem.eql(u8, arg, "--dbfilename")) {
                const value = args.next() orelse return error.MissingDbFilenameValue;
                config.db_filename = try allocator.dupe(u8, value);
            }
        }

        return config;
    }

    fn setReplica(self: *Config, val: []const u8) !void {
        var tokens = std.mem.tokenizeScalar(u8, val, ' ');
        const host = tokens.next() orelse return error.InvalidReplicaOfValue;
        const port_str = tokens.next() orelse return error.InvalidReplicaOfValue;
        if (tokens.next() != null) return error.InvalidReplicaOfValue;

        const port = try std.fmt.parseInt(u16, port_str, 10);
        const host_copy = try self.allocator.dupe(u8, host);

        self.role = .replica;
        self.replica = .{ .host = host_copy, .port = port };
    }

    pub fn deinit(self: Config) void {
        if (self.dir.ptr != ".".ptr) self.allocator.free(self.dir);
        if (self.db_filename.ptr != "dump.rdb".ptr) self.allocator.free(self.db_filename);
        if (self.replica) |r| self.allocator.free(r.host);
    }
};
