const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;

pub const Header = @import("serde/message.zig").Header;
pub const MessageFlag = @import("serde/message.zig").MessageFlag;
pub const MessageDecoder = @import("serde/message.zig").MessageDecoder;
pub const Message = @import("serde/message.zig").Message;
pub const Handshake = @import("serde/handshake.zig").Handshake;
pub const Event = @import("serde/handshake.zig").Event;
pub const RawMessage = @import("serde/message.zig").RawMessage;
pub const MessagePool = @import("serde/message_pool.zig").MessagePool;
pub const concatenateSlicesAlloc = @import("serde/slices.zig").concatenateSlicesAlloc;
pub const concatenateSlicesAllocZ = @import("serde/slices.zig").concatenateSlicesAllocZ;
pub const concatenateSlicesBuf = @import("serde/slices.zig").concatenateSlicesBuf;
pub const createHandshakeMessage = @import("serde/message.zig").createHandshakeMessage;
pub const createEventMessage = @import("serde/message.zig").createEventMessage;

pub const MAX_BYTES_MESSAGE: meta.FieldType(Header, .msg_len) = std.math.maxInt(meta.FieldType(Header, .msg_len));
pub const HEADER_OFFSET_START: u32 = 0;
pub const HEADER_OFFSET_END: u32 = HEADER_SIZE;
pub const HEADER_SIZE: u32 = Header.size;

pub const PLACEHOLDER = @import("serde/message.zig").PLACEHOLDER;
pub const SECTION_SEPARATOR = @import("serde/message.zig").SECTION_SEPARATOR;
pub const SECTION_SEPARATOR_SEQ = @import("serde/message.zig").SECTION_SEPARATOR_SEQ;
pub const UNIT_SEPARATOR = @import("serde/message.zig").UNIT_SEPARATOR;
pub const UNIT_SEPARATOR_SEQ = @import("serde/message.zig").UNIT_SEPARATOR_SEQ;

test {
    _ = @import("serde/message.zig");
    // _ = @import("serde/slices.zig");
    // _ = @import("serde/message_pool.zig");
}
