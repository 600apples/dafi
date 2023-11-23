const std = @import("std");
const serde = @import("../serde.zig");
const ascii = std.ascii;
const Allocator = std.mem.Allocator;
const expect = std.testing.expect;
const assert = std.debug.assert;

pub const ToRawMessageIterator = @This();

data: []const u8,
uuid: u16,
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
        .allocator = allocator,
    };
}

pub fn reset(self: *ToRawMessageIterator) void {
    defer assert(@atomicRmw(bool, &self.lock, .Xchg, false, .SeqCst)); // unlock the iterator
    self.allocator.free(self.data);
    self.uuid = self.uuid +% 1;
    self.data = undefined;
}

pub fn make(self: *ToRawMessageIterator, data: []const u8, transmitter: []const u8, receiver: []const u8, func_name: []const u8, timeout: ?u16, return_result: bool, metadata: ?[]const u8, flag: serde.MessageFlag) ![]const u8 {
    while (@atomicRmw(bool, &self.lock, .Xchg, true, .SeqCst)) {} // two threads should not prepare at the same time
    errdefer assert(@atomicRmw(bool, &self.lock, .Xchg, false, .SeqCst));

    const infered_metadata = metadata orelse "";
    const metadata_len = infered_metadata.len;
    const msg_len = data.len + metadata_len;
    if (msg_len > serde.MAX_BYTES_MESSAGE) {
        return error.ExceedsMaxMessageize;
    }
    if (metadata_len > serde.MAX_BYTES_METADATA) {
        return error.ExceedsMaxMetadataSize;
    }
    const header: serde.Header = .{ .uuid = self.uuid, .msg_len = @as(u26, @truncate(msg_len)), .metadata_len = @as(u8, @truncate(metadata_len)), .transmitter = serde.hashString(transmitter), .receiver = serde.hashString(receiver), .func_name = serde.hashString(func_name), .timeout = timeout orelse 0, .return_result = return_result, .flag = flag };
    var backed_header_bytes: [serde.HEADER_SIZE]u8 = undefined;
    header.toBytes(&backed_header_bytes);
    const concatenated = try serde.concatenateSlicesAlloc(self.allocator, &[_][]const u8{ &backed_header_bytes, infered_metadata, data });
    self.data = concatenated;
    return concatenated;
}

test "test to raw messages iterator 2 messages" {
    var alloc = std.testing.allocator;
    var iterator = try ToRawMessageIterator.init(alloc);

    // first message
    var message1 = try iterator.make("hello world", "transmitter", "receiver", "func_name", 777, false, null, .EMPTY);
    const restored_header1 = serde.Header.fromMessage(message1);

    // only one chunk so chunk_len == msg_len
    try std.testing.expect(restored_header1.msg_len == 11);
    try std.testing.expect(restored_header1.transmitter == 28269);
    try std.testing.expect(restored_header1.receiver == 43172);
    try std.testing.expect(restored_header1.func_name == 41305);
    try std.testing.expect(restored_header1.return_result == false);
    try std.testing.expect(restored_header1.flag == .EMPTY);
    try std.testing.expect(restored_header1.timeout == 777);
    iterator.reset();

    // // second message
    var message2 = try iterator.make("hello world@", "transmitter@", "receiver@", "func_name@", 555, true, "metadata", .EMPTY);
    const restored_header2 = serde.Header.fromMessage(message2);

    // only one chunk so chunk_len == msg_len
    try std.testing.expect(restored_header2.msg_len == 20);

    try std.testing.expect(restored_header2.transmitter == 36344);
    try std.testing.expect(restored_header2.receiver == 33548);
    try std.testing.expect(restored_header2.func_name == 59636);
    try std.testing.expect(restored_header2.return_result == true);
    try std.testing.expect(restored_header2.flag == .EMPTY);
    try std.testing.expect(restored_header2.timeout == 555);
    iterator.reset();
}

test "test to raw messages iterator big message message" {
    var alloc = std.testing.allocator;
    var iterator = try ToRawMessageIterator.init(alloc);
    defer iterator.reset();

    var data: []const u8 = "friedeggs|" ** 12e5;
    var metadata: []const u8 = "metadata";
    var message = try iterator.make(data, "transmitter", "receiver", "func_name", 777, false, metadata, .EMPTY);

    var total_len: usize = data.len + metadata.len;
    var friedeggs_occ: usize = 0;

    var restored_header = serde.Header.fromMessage(message);
    try std.testing.expect(restored_header.transmitter == 28269);
    try std.testing.expect(restored_header.receiver == 43172);
    try std.testing.expect(restored_header.func_name == 41305);
    try std.testing.expect(restored_header.timeout == 777);
    try std.testing.expect(restored_header.return_result == false);
    try std.testing.expect(restored_header.flag == .EMPTY);
    try std.testing.expect(restored_header.msg_len == total_len);
    var payload_iterator = std.mem.split(u8, message, "|");
    while (payload_iterator.next()) |payload| {
        friedeggs_occ += if (std.ascii.indexOfIgnoreCase(payload, "gg")) |_| 1 else 0;
    }
    try std.testing.expect(total_len + serde.HEADER_SIZE == message.len);
    try std.testing.expect(friedeggs_occ == 12e5);
}
