const std = @import("std");
const serde = @import("../serde.zig");

const os = std.os;
const net = std.net;
const ascii = std.ascii;
const Allocator = std.mem.Allocator;
const expect = std.testing.expect;
const Header = serde.Header;
const Metadata = serde.Metadata;
const Message = serde.Message;
const RawMessage = serde.RawMessage;
const MessageFlag = serde.MessageFlag;
const MessageDecoder = serde.MessageDecoder;
const print = std.debug.print;

pub const StreamReadError = error{
    IncompleteMessage,
    EOF,
};

fn mustRead(reader: anytype, buf: []u8) !void {
    var n: usize = try reader.read(buf);
    if (n == 0) {
        return StreamReadError.EOF;
    } else if (n > buf.len) {
        return StreamReadError.IncompleteMessage;
    }
}

pub const MessagePool = struct {
    allocator: Allocator,
    // web_connection: ?*WebConnection,
    messages_delta: usize = 0,

    const Self = @This();

    pub fn init(allocator: Allocator) !*Self {
        var self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            // .web_connection = null,
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        // if (self.web_connection) |web_connection| web_connection.deinit();
        self.allocator.destroy(self);
    }

    pub fn sendMessage(
        self: *Self,
        writer: anytype,
        data: []const u8,
        uuid: u16,
        flag: MessageFlag,
        decoder: MessageDecoder,
        is_bytes: bool,
        return_result: bool,
        transmitter: ?[]const u8,
        receiver: ?[]const u8,
        func_name: ?[]const u8,
    ) !void {
        const raw_message = try RawMessage.create(self.allocator, data, uuid, flag, decoder, is_bytes, return_result, transmitter, receiver, func_name);
        defer self.allocator.free(raw_message.data);
        try raw_message.sendTo(writer);
        if (return_result) self.messages_delta += 1;
    }

    pub fn createMessage(
        _: *Self,
        allocator: Allocator,
        data: []const u8,
        uuid: u16,
        flag: MessageFlag,
        decoder: MessageDecoder,
        is_bytes: bool,
        return_result: bool,
        transmitter: ?[]const u8,
        receiver: ?[]const u8,
        func_name: ?[]const u8,
    ) ![]u8 {
        return (try RawMessage.create(allocator, data, uuid, flag, decoder, is_bytes, return_result, transmitter, receiver, func_name)).data;
    }

    pub fn sendSynMessage(_: *Self, writer: anytype) !void {
        // Create and send SYN message. This message is used to distinguish between websockets and raw TCP connections.
        const fulfillment: [serde.HEADER_SIZE]u8 = [_]u8{ascii.control_code.us} ** serde.HEADER_SIZE;
        _ = try writer.write(&fulfillment);
    }

    pub fn tryAuthenticate(_: *Self, buf: []u8, password: []const u8) !void {
        if (std.mem.eql(u8, password, "")) return;
        return if (std.mem.startsWith(u8, buf, password)) {} else error.PasswordMismatch;
    }

    pub fn receiveMessage(self: *Self, reader: anytype) !*Message {
        var header_buf: [serde.HEADER_SIZE]u8 = undefined;
        try mustRead(reader, &header_buf);
        const header = Header.fromBytes(&header_buf);
        const msg_len = header.msg_len + header_buf.len;
        const data_buf = try self.allocator.alloc(u8, msg_len);
        errdefer self.allocator.free(data_buf);
        @memcpy(data_buf, &header_buf);
        try mustRead(reader, data_buf[header_buf.len..]);
        self.messages_delta -= 1;
        return try Message.create(self.allocator, header, data_buf);
    }

    pub fn receiveWsMessage(self: *Self, reader: anytype) !?*Message {
        var data = (try reader.readNoBuffer()) orelse return null;
        return try self.buildWsMessage(data);
    }

    pub fn buildWsMessage(self: *Self, data: []const u8) !*Message {
        const header = Header.fromBytes(data[0..serde.HEADER_SIZE]);
        const data_buf = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_buf);
        return try Message.create(self.allocator, header, data_buf);
    }
};
//
// test "test message fields" {
//     var allocator = std.testing.allocator;
//     var buf: [120]u8 = undefined;
//     var bufstream = std.io.fixedBufferStream(&buf);
//
//     var msgpool = try MessagePool.init(allocator);
//     defer msgpool.deinit();
//     _ = try msgpool.sendMessage(&bufstream, "hello world@", .REQUEST, .JSON, false, 12345, "receiver@", "func_name@", 555, true, "metadata");
//     bufstream.reset();
//     var msg = try msgpool.receiveMessage(bufstream.reader());
//     defer msg.deinit();
//     bufstream.reset();
//     try expect(msg.hasReceiver());
//     try msg.setFields(&[_]Message.Entry{ .{ .flag = .BROADCAST }, .{ .transmitter = 12345 }, .{ .receiver = 12345 }, .{ .func_name = "func_name" }, .{ .uuid = 11111 }, .{ .timeout = 999 }, .{ .payload = "**************************************" } });
//     msg.setDecoder(.PICKLE);
//     _ = try bufstream.write(msg.data);
//     bufstream.reset();
//     var msg2 = try msgpool.receiveMessage(bufstream.reader());
//     defer msg2.deinit();
//
//     try expect(msg2.getFlag() == .BROADCAST);
//     try expect(msg2.getDecoder() == .PICKLE);
//     try expect(msg2.getTransmitter() == 12345);
//     try expect(msg2.getReceiver() == 12345);
//     try expect(msg2.getTimeout() == 999);
//     try expect(msg2.getFuncName() == 41305);
//     try expect(std.mem.eql(u8, msg2.getPayload(), "**************************************"));
//     try expect(std.mem.eql(u8, msg2.getMetadataContext(), "metadata"));
// }
//
// test "test message fields empty fields" {
//     var allocator = std.testing.allocator;
//     var buf: [120]u8 = undefined;
//     var bufstream = std.io.fixedBufferStream(&buf);
//
//     var msgpool = try MessagePool.init(allocator);
//     defer msgpool.deinit();
//
//     _ = try msgpool.sendMessage(&bufstream, "node1☆result1★node2☆result1☆result2☆result3☆result4★node3☆result1", .REQUEST, .JSON, false, 0, "", "func_name@", 555, true, "metadata");
//     bufstream.reset();
//     var msg = try msgpool.receiveMessage(bufstream.reader());
//     defer msg.deinit();
//     bufstream.reset();
//     try expect(!msg.hasReceiver());
//
//     try expect(msg.getFlag() == .REQUEST);
//     try expect(msg.getTransmitter() == 0);
//     try expect(msg.getReceiver() == 0);
//     try expect(msg.getTimeout() == 555);
//     try expect(msg.getFuncName() == 59636);
//     try expect(std.mem.eql(u8, msg.getPayload(), "node1☆result1★node2☆result1☆result2☆result3☆result4★node3☆result1"));
//
//     // var iterator = msg.dataIterator();
//     // while (iterator.next()) |entry| {
//     //     std.debug.print("{s} - {}\n", .{entry.value, entry.is_unit});
//     // }
// }
