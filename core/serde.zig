const std = @import("std");

pub const Header = @import("serde/message.zig").Header;
pub const MessageFlag = @import("serde/message.zig").MessageFlag;
pub const Message = @import("serde/message.zig").Message;
pub const ToRawMessageIterator = @import("serde/ToRawMessageIterator.zig").ToRawMessageIterator;
pub const concatenateSlicesAlloc = @import("serde/slices.zig").concatenateSlicesAlloc;

// 4 MiB + delimiters + header
pub const MAX_BYTES_CHUNK: u23 = 4 * 1024 * 1024 + HEADER_OFFSET_END;
pub const MAX_BYTES_MESSAGE: u32 = std.math.maxInt(u26);
pub const MESSAGE_DELIMITER = "★";
pub const METADATA_DELIMITER = "☆";
pub const HEADER_OFFSET_START: u32 = MESSAGE_DELIMITER.len;
pub const HEADER_OFFSET_END: u32 = MESSAGE_DELIMITER.len + HEADER_SIZE;
pub const HEADER_SIZE: u32 = Header.size;


pub fn hashString(s: []const u8) u16 {
    return @as(u16, @truncate(std.hash.Wyhash.hash(0, s)));
}

test {
    _ =  @import("serde/message.zig");
    _ =  @import("serde/ToRawMessageIterator.zig");
    _ = @import("serde/slices.zig");
}
