const std = @import("std");
const ascii = std.ascii;
const Allocator = std.mem.Allocator;

pub const Header = @import("serde/message.zig").Header;
pub const MessageFlag = @import("serde/message.zig").MessageFlag;
pub const Message = @import("serde/message.zig").Message;
pub const ToRawMessageIterator = @import("serde/ToRawMessageIterator.zig");
pub const FromRawMessageIterator = @import("serde/FromRawMessageIterator.zig");
pub const concatenateSlicesAlloc = @import("serde/slices.zig").concatenateSlicesAlloc;

pub const MAX_BYTES_MESSAGE: u26 = std.math.maxInt(u26);
pub const MAX_BYTES_METADATA: u8 = std.math.maxInt(u8);
pub const HEADER_OFFSET_START: u32 = 0;
pub const HEADER_OFFSET_END: u32 = HEADER_SIZE;
pub const HEADER_SIZE: u32 = Header.size;

pub fn hashString(s: []const u8) u16 {
    return @as(u16, @truncate(std.hash.Wyhash.hash(0, s)));
}

pub const MessagePool = struct {
    allocator: Allocator,
    to_raw_message_iterator: ToRawMessageIterator,
    from_raw_message_iterator: FromRawMessageIterator,

    pub fn init(allocator: Allocator) !MessagePool {
        return .{
            .allocator = allocator,
            .to_raw_message_iterator = try ToRawMessageIterator.init(allocator),
            .from_raw_message_iterator = FromRawMessageIterator.init(allocator),
        };
    }

    pub fn send(self: *MessagePool, writer: anytype, data: []const u8, transmitter: []const u8, receiver: []const u8, func_name: []const u8, timeout: ?u16, return_result: bool, metadata: ?[]const u8, flag: MessageFlag) !void {
        defer self.to_raw_message_iterator.reset();
        const message = try self.to_raw_message_iterator.make(data, transmitter, receiver, func_name, timeout, return_result, metadata, flag);
        try writer.write(message);
    }

    pub fn receive(self: *MessagePool, reader: anytype) !?Message {
        return try self.from_raw_message_iterator.next(reader);
    }
};

test {
    _ = @import("serde/message.zig");
    _ = @import("serde/ToRawMessageIterator.zig");
    _ = @import("serde/slices.zig");
}
