const std = @import("std");
const c = @cImport({
    @cInclude("time.h");
    @cInclude("sys/time.h");
});

pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const writer = std.io.getStdErr().writer();

    const level_txt = switch (level) {
        .err => "\x1b[31;1mERROR\x1b[0m", // Bold Red
        .warn => "\x1b[33;1mWARN\x1b[0m", // Bold Yellow
        .info => "\x1b[32;1mINFO\x1b[0m", // Bold Green
        .debug => "\x1b[34;1mDEBUG\x1b[0m", // Bold Blue
    };

    var tv: c.timeval = undefined;
    _ = c.gettimeofday(&tv, null);

    const timeinfo = c.localtime(&tv.tv_sec);
    var tms_buf: [32]u8 = undefined;
    const bytes_written = c.strftime(&tms_buf[0], tms_buf.len, "%Y-%m-%d %H:%M:%S", timeinfo);
    const datetime_str = if (bytes_written > 0) tms_buf[0..bytes_written] else "unknown time";
    const ms = @divFloor(tv.tv_usec, 1000);

    writer.print("[{s}.{d}] [{s}] ({s}) ", .{ datetime_str, ms, level_txt, @tagName(scope) }) catch return;
    writer.print(format, args) catch return;
    writer.print("\n", .{}) catch return;
}
