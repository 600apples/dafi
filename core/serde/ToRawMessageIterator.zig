const std = @import("std");
const serde = @import("../serde.zig");
const Allocator = std.mem.Allocator;
const expect = std.testing.expect;
const assert = std.debug.assert;

const ToRawMessageIterator = @This();

data: []const u8,
uuid: u16,
transmitter: u16,
receiver: u16,
func_name: u16,
timeout: u16,
return_result: bool,
start_position: usize = 0,
msg_len: u26,
flag: serde.MessageFlag,
allocator: Allocator,
lock: bool = false,

pub fn init(allocator: Allocator) !ToRawMessageIterator {
    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.os.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    return .{
        .uuid = prng.random().int(u16),
        .data = undefined,
        .transmitter = undefined,
        .receiver = undefined,
        .func_name = undefined,
        .timeout = undefined,
        .return_result = undefined,
        .start_position = undefined,
        .msg_len = undefined,
        .flag = undefined,
        .allocator = allocator,
    };
}

pub fn reset(self: *ToRawMessageIterator) void {
    defer assert(@atomicRmw(bool, &self.lock, .Xchg, false, .SeqCst)); // unlock the iterator
    self.allocator.free(self.data);
    self.data = undefined;
    self.transmitter = undefined;
    self.receiver = undefined;
    self.func_name = undefined;
    self.timeout = undefined;
    self.return_result = undefined;
    self.start_position = undefined;
    self.msg_len = undefined;
    self.flag = undefined;
}

pub fn prepare(self: *ToRawMessageIterator, data: []const u8, transmitter: []const u8, receiver: []const u8, func_name: []const u8, timeout: ?u16, return_result: bool, metadata: ?[]const u8, flag: serde.MessageFlag) !void {
    while (@atomicRmw(bool, &self.lock, .Xchg, true, .SeqCst)) {} // two threads should not prepare at the same time
    const concatenated = try serde.concatenateSlicesAlloc(self.allocator, &[_][]const u8{ data, serde.METADATA_DELIMITER, metadata orelse "" });
    var chunk_iterator_cycles = @divFloor(concatenated.len, serde.MAX_BYTES_CHUNK);
    if (chunk_iterator_cycles > 0) {
        if (@rem(concatenated.len, serde.MAX_BYTES_CHUNK) != 0) {
            // One partial chunk is needed to send the last message
            chunk_iterator_cycles += 1;
        }
    } else {
        // If the message is smaller than the chunk size, we still need to send it
        chunk_iterator_cycles = 1;
    }
    const msg_len: u26 = @truncate(chunk_iterator_cycles * serde.HEADER_OFFSET_END + concatenated.len);
    if (msg_len > serde.MAX_BYTES_MESSAGE) {
        defer assert(@atomicRmw(bool, &self.lock, .Xchg, false, .SeqCst));
        return error.ExceedsMaxMessageize;
    }
    self.data = concatenated;
    self.uuid = self.uuid +% 1;
    self.transmitter = serde.hashString(transmitter);
    self.receiver = serde.hashString(receiver);
    self.func_name = serde.hashString(func_name);
    self.timeout = timeout orelse 0;
    self.return_result = return_result;
    self.start_position = 0;
    self.msg_len = msg_len;
    self.flag = flag;
}

fn next(self: *ToRawMessageIterator) !?[]const u8 {
    return if (self.start_position >= self.data.len) null else blk: {
        const end_position = self.start_position + @min(self.data.len - self.start_position, serde.MAX_BYTES_CHUNK);
        const data_chunk = self.data[self.start_position..end_position];
        const chunk_len: u23 = @truncate(data_chunk.len + serde.HEADER_OFFSET_END);
        self.start_position = end_position;
        const header: serde.Header = .{ .uuid = self.uuid, .transmitter = self.transmitter, .receiver = self.receiver, .func_name = self.func_name, .timeout = self.timeout, .return_result = self.return_result, .complete = self.start_position >= self.data.len, .chunk_len = chunk_len, .msg_len = self.msg_len, .flag = self.flag };
        var backed_header_bytes: [serde.HEADER_SIZE]u8 = undefined;
        header.toBytes(&backed_header_bytes);
        const result = try serde.concatenateSlicesAlloc(self.allocator, &[_][]const u8{ serde.MESSAGE_DELIMITER, &backed_header_bytes, data_chunk });
        break :blk result;
    };
}

test "header to int" {
    var header = serde.Header{
        .uuid = 233,
        .transmitter = 456,
        .receiver = 0,
        .func_name = 333,
        .complete = true,
        .return_result = false,
        .timeout = 777,
        .chunk_len = 777,
        .msg_len = 777,
        .flag = .EMPTY,
    };

    var backed_int = header.toInt();
    try expect(backed_int == 0x612000c260309014d000001c800e9);

    var restored = serde.Header.fromInt(backed_int);
    try std.testing.expectEqual(header, restored);
}

test "header to bytes" {
    var header = serde.Header{
        .uuid = 343,
        .transmitter = 111,
        .receiver = 66,
        .func_name = 788,
        .complete = true,
        .return_result = false,
        .timeout = 777,
        .chunk_len = 777,
        .msg_len = 777,
        .flag = .EMPTY,
    };

    var backed_bytes: [serde.HEADER_SIZE]u8 = undefined;
    header.toBytes(&backed_bytes);

    var restored = serde.Header.fromBytes(&backed_bytes);
    try std.testing.expectEqual(header, restored);
}

test "test to raw messages iterator 2 messages" {
    var alloc = std.testing.allocator;
    var iterator = try ToRawMessageIterator.init(alloc);

    // first message
    try iterator.prepare("hello world", "transmitter", "receiver", "func_name", 777, false, null, .EMPTY);
    var message1 = try iterator.next() orelse unreachable;
    try std.testing.expect(try iterator.next() == null);
    iterator.reset();
    defer alloc.free(message1);

    var header_bytes1 = message1[serde.HEADER_OFFSET_START..serde.HEADER_OFFSET_END];
    var restored1 = serde.Header.fromBytes(header_bytes1);

    try std.testing.expect(restored1.transmitter == 28269);
    try std.testing.expect(restored1.receiver == 43172);
    try std.testing.expect(restored1.func_name == 41305);
    try std.testing.expect(restored1.complete == true);
    try std.testing.expect(restored1.return_result == false);
    try std.testing.expect(restored1.flag == .EMPTY);
    try std.testing.expect(restored1.timeout == 777);
    // only one chunk so chunk_len == msg_len
    try std.testing.expect(restored1.chunk_len == restored1.msg_len);

    // second message
    try iterator.prepare("hello world@", "transmitter@", "receiver@", "func_name@", 555, true, "metadata", .EMPTY);

    var message2 = try iterator.next() orelse unreachable;
    try std.testing.expect(try iterator.next() == null);
    iterator.reset();
    defer alloc.free(message2);

    var header_bytes2 = message2[serde.HEADER_OFFSET_START..serde.HEADER_OFFSET_END];
    var restored2 = serde.Header.fromBytes(header_bytes2);

    try std.testing.expect(restored2.transmitter == 36344);
    try std.testing.expect(restored2.receiver == 33548);
    try std.testing.expect(restored2.func_name == 59636);
    try std.testing.expect(restored2.complete == true);
    try std.testing.expect(restored2.return_result == true);
    try std.testing.expect(restored2.flag == .EMPTY);
    try std.testing.expect(restored2.timeout == 555);
    // only one chunk so chunk_len == msg_len
    try std.testing.expect(restored2.chunk_len == restored2.msg_len);
}

test "test to raw messages iterator multichunk message" {
    var alloc = std.testing.allocator;
    var iterator = try ToRawMessageIterator.init(alloc);
    defer iterator.reset();

    var data: []const u8 = "friedeggs|" ** 12e5;
    try iterator.prepare(data, "transmitter", "receiver", "func_name", 777, false, "metadata", .EMPTY);

    var message_count: usize = 0;
    var total_len: usize = 0;
    var friedeggs_occ: usize = 0;
    while (try iterator.next()) |message| {
        defer alloc.free(message);
        message_count += 1;
        var header_bytes = message[serde.HEADER_OFFSET_START..serde.HEADER_OFFSET_END];
        var restored = serde.Header.fromBytes(header_bytes);
        total_len += restored.chunk_len;
        try std.testing.expect(restored.transmitter == 28269);
        try std.testing.expect(restored.receiver == 43172);
        try std.testing.expect(restored.func_name == 41305);
        try std.testing.expect(restored.timeout == 777);
        try std.testing.expect(restored.return_result == false);
        try std.testing.expect(restored.flag == .EMPTY);

        if (message_count == 3) {
            try std.testing.expect(restored.complete == true);
            try std.testing.expect(restored.msg_len == total_len);
        } else {
            try std.testing.expect(restored.complete == false);
        }

        var payload_iterator = std.mem.split(u8, message, "|");
        while (payload_iterator.next()) |payload| {
            friedeggs_occ += if (std.ascii.indexOfIgnoreCase(payload, "ie")) |_| 1 else 0;
        }
    }
    try std.testing.expect(total_len == 12000071);
    try std.testing.expect(friedeggs_occ == 12e5);
}
