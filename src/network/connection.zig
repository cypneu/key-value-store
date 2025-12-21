const std = @import("std");

const resp_parser = @import("resp/parser.zig");
const StreamParser = resp_parser.StreamParser;
const ParseResult = StreamParser.ParseResult;
const MessageKind = StreamParser.MessageKind;

pub const FrameType = enum {
    command,
    fullresync,
    rdb_snapshot,
};

pub const Frame = struct {
    consumed: usize,
    parts: []const []const u8,
    type: FrameType,
    raw: []const u8,
};

pub const Connection = struct {
    id: u64,
    buffer: std.ArrayList(u8),
    cursor: usize,
    parser: StreamParser,
    parse_error: bool = false,

    pub fn init(allocator: std.mem.Allocator, id: u64) Connection {
        return .{
            .id = id,
            .buffer = std.ArrayList(u8).init(allocator),
            .cursor = 0,
            .parser = StreamParser.init(allocator),
            .parse_error = false,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.buffer.deinit();
        self.parser.deinit();
    }

    pub fn feedBytes(self: *Connection, bytes: []const u8) !void {
        try self.buffer.appendSlice(bytes);
    }

    pub fn nextFrame(self: *Connection) ?Frame {
        const available = self.buffer.items[self.cursor..];
        const result = self.parser.parse(available);

        return switch (result) {
            .NeedMore => null,
            .Error => {
                self.parse_error = true;
                return null;
            },
            .Done => |done| Frame{
                .consumed = done.consumed,
                .parts = done.parts,
                .type = classifyFrame(done.kind, done.parts),
                .raw = available[0..done.consumed],
            },
        };
    }

    pub fn consume(self: *Connection, bytes: usize) void {
        self.cursor += bytes;
        self.parser.reset();
        self.maybeCompact();
    }

    fn maybeCompact(self: *Connection) void {
        if (self.cursor == self.buffer.items.len) {
            self.buffer.clearRetainingCapacity();
            self.cursor = 0;
        }
    }

    pub fn isBufferEmpty(self: *Connection) bool {
        return self.cursor == self.buffer.items.len;
    }
};

fn classifyFrame(kind: MessageKind, parts: []const []const u8) FrameType {
    return switch (kind) {
        .SimpleString => if (parts.len > 0 and std.mem.startsWith(u8, parts[0], "FULLRESYNC")) .fullresync else .command,
        .TopLevelBulk => .rdb_snapshot,
        else => .command,
    };
}

pub const ConnectionRegistry = struct {
    allocator: std.mem.Allocator,
    connections: std.AutoHashMap(u64, Connection),
    next_id: u64,

    pub fn init(allocator: std.mem.Allocator) ConnectionRegistry {
        return .{
            .allocator = allocator,
            .connections = std.AutoHashMap(u64, Connection).init(allocator),
            .next_id = 0,
        };
    }

    pub fn deinit(self: *ConnectionRegistry) void {
        var it = self.connections.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.connections.deinit();
    }

    pub fn create(self: *ConnectionRegistry) !u64 {
        const id = self.next_id;
        self.next_id += 1;

        try self.connections.put(id, Connection.init(self.allocator, id));
        return id;
    }

    pub fn get(self: *ConnectionRegistry, id: u64) ?*Connection {
        return self.connections.getPtr(id);
    }

    pub fn remove(self: *ConnectionRegistry, id: u64) ?Connection {
        if (self.connections.fetchRemove(id)) |kv| {
            return kv.value;
        }
        return null;
    }

    pub fn iterator(self: *ConnectionRegistry) std.AutoHashMap(u64, Connection).Iterator {
        return self.connections.iterator();
    }
};
