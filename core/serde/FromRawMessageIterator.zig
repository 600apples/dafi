const std = @import("std");
const serde = @import("../serde.zig");
const ascii = std.ascii;
const StringHashMap = std.StringHashMap;
const Allocator = std.mem.Allocator;

pub const FromRawMessageIterator = @This();

allocator: Allocator,

pub fn init(allocator: Allocator) FromRawMessageIterator {
    return .{
        .allocator = allocator,
    };
}

pub fn next(self: *FromRawMessageIterator, reader: anytype) !?serde.Message {
    var header_buf: [serde.HEADER_SIZE]u8 = undefined;
    const n = try reader.read(&header_buf);
    if (n == 0) return null;

    const header = serde.Header.fromBytes(&header_buf);
    const data_buf = try self.allocator.alloc(u8, header.msg_len);
    _ = try reader.read(data_buf);
    return serde.Message{
        .header = header,
        .data = data_buf,
        .allocator = self.allocator,
    };
}
