const std = @import("std");
const buffer = @import("../web/buffer.zig");
const framing = @import("../web/framing.zig");
const Fragmented = framing.Fragmented;
const Allocator = std.mem.Allocator;

pub const MessageType = enum {
    text,
    binary,
    close,
    ping,
    pong,
};

pub const Message = struct {
    type: MessageType,
    data: []const u8,
};

const ParsePhase = enum {
    pre,
    header,
    payload,
};

pub const Reader = struct {
    // Where we go to get buffers. Hides the buffer-getting details, which is
    // based on both how we're configured as well as how large a buffer we need
    bp: *buffer.Provider,

    // Start position in buf of active data
    start: usize,

    // Length of active data. Can span more than one message
    // buf[start..start + len]
    len: usize,

    // Length of active message. message_len is always <= len
    // buf[start..start + message_len]
    message_len: usize,

    // Maximum supported message size
    max_size: usize,

    // The current buffer, can reference static, a buffer from the pool, or some
    // dynamically allocated memory
    buf: buffer.Buffer,

    // Our static buffer. Initialized upfront.
    static: buffer.Buffer,

    // If we're dealing with a fragmented message (websocket fragment, not tcp
    // fragment), the state of the fragmented message is maintained here.)
    fragment: ?Fragmented,

    pub fn init(buffer_size: usize, max_size: usize, bp: *buffer.Provider) !Reader {
        const static = try bp.static(buffer_size);

        return .{
            .bp = bp,
            .len = 0,
            .start = 0,
            .buf = static,
            .static = static,
            .message_len = 0,
            .fragment = null,
            .max_size = max_size,
        };
    }

    pub fn deinit(self: *Reader) void {
        if (self.fragment) |f| {
            f.deinit();
        }

        if (self.buf.type != .static) {
            self.bp.free(self.buf);
        }

        // the reader owns static, when it goes, static goes
        self.bp.free(self.static);
    }

    pub fn handled(self: *Reader) void {
        if (self.fragment) |f| {
            f.deinit();
            self.fragment = null;
        }
    }

    pub fn readMessage(self: *Reader, stream: anytype) !Message {
        // Our inner loop reads 1 websocket frame, which may not form a whole message
        // due to the fact that websocket has its own annoying fragmentation thing
        // going on.
        // Besides an error, we only want to return from here if we have a full message
        // which would either be:
        //  - a control frame within a fragmented message,
        //  - a fragmented message that we have all the pieces to,
        //  - a single frame (control or otherwise) that forms a full message
        //    (this last one is the most common case)
        outer: while (true) {
            var data_needed: usize = 2; // always need at least the first two bytes to start figuring things out
            var phase = ParsePhase.pre;
            var header_length: usize = 0;
            var length_of_length: usize = 0;

            var masked = true;
            var is_continuation = false;
            var message_type: MessageType = undefined;

            self.prepareForNewMessage();

            while (true) {
                if ((try self.read(stream, data_needed)) == false) {
                    return error.Closed;
                }

                switch (phase) {
                    .pre => {
                        const msg = self.currentMessage();
                        const byte1 = msg[0];
                        const byte2 = msg[1];
                        masked = byte2 & 128 == 128;
                        length_of_length = switch (byte2 & 127) {
                            126 => 2,
                            127 => 8,
                            else => 0,
                        };
                        phase = ParsePhase.header;
                        header_length = 2 + length_of_length;
                        if (masked) {
                            header_length += 4;
                        }
                        data_needed = header_length;

                        switch (byte1 & 15) {
                            0 => is_continuation = true,
                            1 => message_type = .text,
                            2 => message_type = .binary,
                            8 => message_type = .close,
                            9 => message_type = .ping,
                            10 => message_type = .pong,
                            else => return error.InvalidMessageType,
                        }

                        // FIN, RSV1, RSV2, RSV3, OP,OP,OP,OP
                        // none of the RSV bits should be set
                        if (byte1 & 112 != 0) {
                            return error.ReservedFlags;
                        }

                        if (!is_continuation and length_of_length != 0 and (message_type == .ping or message_type == .close or message_type == .pong)) {
                            return error.LargeControl;
                        }
                    },
                    .header => {
                        const msg = self.currentMessage();
                        const payload_length = switch (length_of_length) {
                            2 => @as(u16, @intCast(msg[3])) | @as(u16, @intCast(msg[2])) << 8,
                            8 => @as(u64, @intCast(msg[9])) | @as(u64, @intCast(msg[8])) << 8 | @as(u64, @intCast(msg[7])) << 16 | @as(u64, @intCast(msg[6])) << 24 | @as(u64, @intCast(msg[5])) << 32 | @as(u64, @intCast(msg[4])) << 40 | @as(u64, @intCast(msg[3])) << 48 | @as(u64, @intCast(msg[2])) << 56,
                            else => msg[1] & 127,
                        };
                        data_needed += payload_length;
                        phase = ParsePhase.payload;
                    },
                    .payload => {
                        const msg = self.currentMessage();
                        const fin = msg[0] & 128 == 128;
                        const payload = msg[header_length..];

                        if (masked) {
                            const mask = msg[header_length - 4 .. header_length];
                            framing.mask(mask, payload);
                        }

                        if (fin) {
                            if (is_continuation) {
                                if (self.fragment) |*f| {
                                    return Message{ .type = f.type, .data = try f.last(payload) };
                                }
                                return error.UnfragmentedContinuation;
                            }

                            if (self.fragment != null and (message_type == .text or message_type == .binary)) {
                                return error.NestedFragment;
                            }

                            // just a normal single-fragment message (most common case)
                            return Message{ .type = message_type, .data = payload };
                        }

                        if (is_continuation) {
                            if (self.fragment) |*f| {
                                try f.add(payload);
                                continue :outer;
                            }
                            return error.UnfragmentedContinuation;
                        } else if (message_type != .text and message_type != .binary) {
                            return error.FragmentedControl;
                        }

                        if (self.fragment != null) {
                            return error.NestedFragment;
                        }
                        self.fragment = try Fragmented.init(self.bp, self.max_size, message_type, payload);
                        continue :outer;
                    },
                }
            }
        }
    }

    fn prepareForNewMessage(self: *Reader) void {
        if (self.buf.type != .static) {
            self.bp.release(self.buf);
            self.buf = self.static;
            self.len = 0;
            self.start = 0;
            return;
        }

        // self.buf is this reader's static buffer, we might have overread
        const message_len = self.message_len;
        self.message_len = 0;
        if (message_len == self.len) {
            // The last read we did got exactly 1 message, no overread. This is good
            // since we can just reset our indexes to 0.
            self.len = 0;
            self.start = 0;
        } else {
            // We overread into the next message.
            self.len -= message_len;
            self.start += message_len;
        }
    }

    // Reads at least to_read bytes and returns true
    // When read fails, returns false
    fn read(self: *Reader, stream: anytype, to_read: usize) !bool {
        const len = self.len;

        if (to_read < len) {
            // we already have to_read bytes available
            self.message_len = to_read;
            return true;
        }

        var buf = self.buf.data;

        // the position in buf up to which we have valid data, this is where
        // we should start filling it up from.
        var pos = self.start + len;

        // how much data we're missing to satifisfy to_read
        const missing = to_read - len;

        if (missing > buf.len - pos) {
            // the position, in buf, where the current message starts
            const start = self.start;

            if (to_read <= buf.len) {
                // We have enough space to read this message in our
                // current buffer, but we need to compact it.
                std.mem.copyForwards(u8, buf[0..], buf[start..pos]);
                self.start = 0;
                pos = len;
            } else if (to_read <= self.max_size) {
                const new_buf = try self.bp.alloc(to_read);
                if (len > 0) {
                    @memcpy(new_buf.data[0..len], buf[start .. start + len]);
                }

                // bp.alloc can return a larger buffer (e.g. from a pool). But we don't
                // want to over-read data here, since we want to be able to cleanly
                // revert back to our static buffer
                buf = new_buf.data[0..to_read];
                pos = len;

                self.start = 0;
                self.buf = new_buf;
                self.len = self.message_len;
            } else {
                return error.TooLarge;
            }
        }

        // Once pos reaches this position, then we have to_read bytes
        const need_pos = pos + missing;

        // A bit arbitrary
        const buf_end = if (to_read > 20) buf.len else need_pos;
        // const buf_end = if (buf.len - need_pos > 20) buf.len else need_pos;

        while (pos < need_pos) {
            const n = try stream.read(buf[pos..buf_end]);
            if (n == 0) {
                return false;
            }
            pos += n;
        }

        self.len = pos - self.start;
        self.message_len = to_read;
        return true;
    }

    fn currentMessage(self: *Reader) []u8 {
        const start = self.start;
        return self.buf.data[start .. start + self.message_len];
    }
};
