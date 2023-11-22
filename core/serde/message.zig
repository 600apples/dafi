const std = @import("std");

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
    transmitter: u16,
    receiver: u16,
    func_name: u16,
    timeout: u16,
    return_result: bool,
    complete: bool,
    chunk_len: u23,
    msg_len: u26,
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
};

pub const Message = struct {
    header: Header,
    data: []const u8,
};
