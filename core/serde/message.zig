const std = @import("std");
const serde = @import("../serde.zig");
const ascii = std.ascii;
const Allocator = std.mem.Allocator;
const expect = std.testing.expect;

pub const MessageFlag = enum(u5) {
    EMPTY,
    HANDSHAKE,
    REQUEST,
    SUCCESS,
    UPDATE_CALLBACKS,
    UNABLE_TO_FIND_CANDIDATE,
    UNABLE_TO_FIND_PROCESS,
    REMOTE_STOPPED_UNEXPECTEDLY,
    SCHEDULER_ERROR,
    SCHEDULER_ACCEPT,
    BROADCAST,
    INIT_STREAM,
    STREAM_ERROR,
    STREAM_THROTTLE,
    RECEIVER_ERROR,
    CONTROLLER_STOPPED_UNEXPECTEDLY,
    PING,
};

pub const Header = packed struct {
    uuid: u16,
    msg_len: u26,
    metadata_len: u8,
    transmitter: u16,
    receiver: u16,
    func_name: u16,
    timeout: u16,
    return_result: bool,
    flag: MessageFlag,

    // guaranteed to be divisible by 8
    pub const size: u32 = @divExact(@bitSizeOf(Header), 8);
    const backed_int_type = std.meta.Int(.unsigned, size * 8);

    pub fn toInt(self: Header) backed_int_type {
        return @as(backed_int_type, @bitCast(self));
    }

    pub fn toBytes(self: Header, buf: []u8) void {
        @memcpy(buf, &@as([size]u8, @bitCast(self)));
    }

    pub fn fromInt(backed_int: backed_int_type) Header {
        return @as(Header, @bitCast(backed_int));
    }

    pub fn fromBytes(backed_bytes: *const [size]u8) Header {
        return @as(Header, @bitCast(backed_bytes.*));
    }

    pub fn fromMessage(message: []const u8) Header {
        // Create header from message payload using well known offset bounds
        const bytes = message[serde.HEADER_OFFSET_START..serde.HEADER_OFFSET_END];
        return serde.Header.fromBytes(bytes);
    }
};

pub const Message = struct {
    header: Header,
    data: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *const Message) void {
        self.allocator.free(self.data);
    }
};

test "header to int" {
    var header = serde.Header{
        .uuid = 233,
        .transmitter = 456,
        .receiver = 0,
        .func_name = 333,
        .return_result = false,
        .timeout = 777,
        .msg_len = 777,
        .metadata_len = 0,
        .flag = .EMPTY,
    };

    var backed_int = header.toInt();
    try expect(backed_int == 0xc240534000007200000030900e9);

    var restored = serde.Header.fromInt(backed_int);
    try std.testing.expectEqual(header, restored);
}

test "header to bytes" {
    var header = serde.Header{
        .uuid = 343,
        .transmitter = 111,
        .receiver = 66,
        .func_name = 788,
        .return_result = false,
        .timeout = 777,
        .msg_len = 777,
        .metadata_len = 0,
        .flag = .EMPTY,
    };

    var backed_bytes: [serde.HEADER_SIZE]u8 = undefined;
    header.toBytes(&backed_bytes);

    var restored = serde.Header.fromBytes(&backed_bytes);
    try std.testing.expectEqual(header, restored);
}
