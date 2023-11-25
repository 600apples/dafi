const std = @import("std");
const serde = @import("../serde.zig");
const Message = serde.Message;
const Allocator = std.mem.Allocator;

const ClientMessageStore = @This();

const buf_size: u16 = 2048;

var buf: [buf_size]?*Message = [_]?*Message{null} ** buf_size;
allocator: Allocator,

pub fn fetch(_: *ClientMessageStore, uuid: u16) ?*Message {
    const hash = @rem(uuid, buf_size);
    if (buf[hash]) |msg| {
        buf[hash] = null;
        return msg;
    }
    return null;
}

pub fn insert(_: *ClientMessageStore, msg: *Message) !void {
    const uuid = msg.getUuid();
    const hash = @rem(uuid, buf_size);
    buf[hash] = msg;
}

pub fn setTimeoutError(self: *ClientMessageStore, uuid: u16) !void {
    if (self.fetch(uuid)) |msg| {
        if (msg.getUuid() == uuid) try msg.writeErrorMessage("Timeout error", .{});
    }
}
