const std = @import("std");
const reader = @import("../web/reader.zig");
const buffer = @import("../web/buffer.zig");
const Reader = reader.Reader;
const MessageType = reader.MessageType;
const Allocator = std.mem.Allocator;

pub const Fragmented = struct {
    type: MessageType,
    max_size: usize,
    buf: std.ArrayList(u8),

    pub fn init(bp: *buffer.Provider, max_size: usize, message_type: MessageType, value: []const u8) !Fragmented {
        var buf = std.ArrayList(u8).init(bp.allocator);
        try buf.ensureTotalCapacity(value.len * 2);
        buf.appendSliceAssumeCapacity(value);

        return .{
            .buf = buf,
            .max_size = max_size,
            .type = message_type,
        };
    }

    pub fn deinit(self: Fragmented) void {
        self.buf.deinit();
    }

    pub fn add(self: *Fragmented, value: []const u8) !void {
        if (self.buf.items.len + value.len > self.max_size) {
            return error.TooLarge;
        }
        try self.buf.appendSlice(value);
    }

    // Optimization so that we don't over-allocate on our last frame.
    pub fn last(self: *Fragmented, value: []const u8) ![]u8 {
        if (self.buf.items.len + value.len > self.max_size) {
            return error.TooLarge;
        }
        try self.buf.ensureUnusedCapacity(value.len);
        self.buf.appendSliceAssumeCapacity(value);
        return self.buf.items;
    }
};

pub const OpCode = enum(u8) {
    text = 128 | 1,
    binary = 128 | 2,
    close = 128 | 8,
    ping = 128 | 9,
    pong = 128 | 10,
};

pub fn mask(m: []const u8, payload: []u8) void {
    @setRuntimeSafety(false);
    const word_size = @sizeOf(usize);

    // not point optimizing this if it's a really short payload
    if (payload.len < word_size) {
        simpleMask(m, payload);
        return;
    }

    // We're going to xor this 1 word at a time.
    // But, our payload's length probably isn't a perfect multiple of word_size
    // so we'll first xor the bits until we have it aligned.
    var data = payload;
    const over = data.len % word_size;

    if (over > 0) {
        simpleMask(m, data[0..over]);
        data = data[over..];
    }

    // shift the mask based on the # bytes we already unmasked in the above loop
    var mask_template: [4]u8 = undefined;
    for (0..4) |i| {
        mask_template[i] = m[(i + over) & 3];
    }

    var i: usize = 0;
    const mask_vector = std.simd.repeat(word_size, @as(@Vector(4, u8), mask_template[0..4].*));
    while (i < data.len) : (i += word_size) {
        var slice = data[i .. i + word_size][0..word_size];
        var masked_data_slice: @Vector(word_size, u8) = slice.*;
        slice.* = masked_data_slice ^ mask_vector;
    }
}

fn simpleMask(m: []const u8, payload: []u8) void {
    @setRuntimeSafety(false);
    for (payload, 0..) |b, i| {
        payload[i] = b ^ m[i & 3];
    }
}

pub fn frame(op_code: OpCode, comptime msg: []const u8) [frameLen(msg)]u8 {
    var framed: [frameLen(msg)]u8 = undefined;
    framed[0] = @intFromEnum(op_code);

    const len = msg.len;
    if (len <= 125) {
        framed[1] = @intCast(len);
        std.mem.copy(u8, framed[2..], msg);
    } else if (len < 65536) {
        framed[1] = 126;
        framed[2] = @intCast((len >> 8) & 0xFF);
        framed[3] = @intCast(len & 0xFF);
        std.mem.copy(u8, framed[4..], msg);
    } else {
        framed[1] = 127;
        framed[2] = @intCast((len >> 56) & 0xFF);
        framed[3] = @intCast((len >> 48) & 0xFF);
        framed[4] = @intCast((len >> 40) & 0xFF);
        framed[5] = @intCast((len >> 32) & 0xFF);
        framed[6] = @intCast((len >> 24) & 0xFF);
        framed[7] = @intCast((len >> 16) & 0xFF);
        framed[8] = @intCast((len >> 8) & 0xFF);
        framed[9] = @intCast(len & 0xFF);
        std.mem.copy(u8, framed[10..], msg);
    }
    return framed;
}

pub fn frameLen(comptime msg: []const u8) usize {
    if (msg.len <= 125) return msg.len + 2;
    if (msg.len < 65536) return msg.len + 4;
    return msg.len + 10;
}
