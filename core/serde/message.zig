const std = @import("std");
const serde = @import("../serde.zig");
const builtin = @import("builtin");
const ascii = std.ascii;
const meta = std.meta;
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const Endian = std.builtin.Endian;
const native_endian = builtin.cpu.arch.endian();
const ArenaAllocator = std.heap.ArenaAllocator;

const expect = std.testing.expect;

pub const PLACEHOLDER = ""[0..];
pub const SECTION_SEPARATOR = '|';
pub const SECTION_SEPARATOR_SEQ: []const u8 = &[_]u8{SECTION_SEPARATOR};
pub const UNIT_SEPARATOR = ',';
pub const UNIT_SEPARATOR_SEQ: []const u8 = &[_]u8{UNIT_SEPARATOR};

const print = std.debug.print;

pub const MessageDecoder = enum(u2) {
    RAW,
    JSON,
    PICKLE,
};

pub const MessageFlag = enum(u4) {
    HANDSHAKE,
    REQUEST,
    RESPONSE,
    ERROR,
    EVENTS,
};

pub fn hashString(s: []const u8) u16 {
    return @as(u16, @truncate(std.hash.Wyhash.hash(0, s)));
}

fn serdeFns(comptime T: type) type {
    return struct {
        // guaranteed to be divisible by 8
        pub const size: u32 = @divExact(@bitSizeOf(T), 8);
        const backed_int_type = std.meta.Int(.unsigned, size * 8);

        pub fn toInt(self: T) backed_int_type {
            return @as(backed_int_type, @bitCast(self));
        }

        pub fn toBytes(self: T, buf: []u8) void {
            @memcpy(buf, &@as([size]u8, @bitCast(self)));
        }

        pub fn fromInt(backed_int: backed_int_type) T {
            return @as(T, @bitCast(backed_int));
        }

        pub fn fromBytes(backed_bytes: *const [size]u8) T {
            return @as(T, @bitCast(backed_bytes.*));
        }

        pub fn setBytes(_: T, comptime field_name: []const u8, v: anytype, buf: []u8) void {
            // Set bytes of field_name in buf to v. the value bit width must be divisible by 8
            // Start position of v assumed to be divisible by 8
            const type_size = @sizeOf(@TypeOf(v));
            const offset_start = @offsetOf(T, field_name);
            const offset_end = offset_start + type_size;
            @memcpy(buf[offset_start..offset_end], &@as([type_size]u8, @bitCast(v)));
        }

        pub fn setBits(_: T, comptime Ftype: type, comptime field_name: []const u8, v: Ftype, buf: []u8) void {
            const bit_offset = @bitOffsetOf(T, field_name);
            std.mem.writePackedIntNative(Ftype, buf, bit_offset, v);
        }
    };
}

pub const Header = packed struct {
    uuid: u16,
    flag: MessageFlag,
    decoder: MessageDecoder,
    is_bytes: bool, // true if bytes or false if string (used for serialization)
    return_result: bool,
    msg_len: u25, // this is not full message length, but only data + metadata length (without header)
    metadata_len: u15,
    msg_parts: u7 = 1, // number of expected receivers for broadcast message. used with Router mode only. // TODO: remove this field
    endian: Endian = native_endian,

    pub usingnamespace serdeFns(Header);

    pub fn create(
        uuid: u16,
        flag: MessageFlag,
        decoder: MessageDecoder,
        is_bytes: bool,
        return_result: bool,
        msg_len: usize,
        metadata_len: usize,
    ) !Header {
        return .{
            .uuid = uuid,
            .flag = flag,
            .decoder = decoder,
            .is_bytes = is_bytes,
            .return_result = return_result,
            .msg_len = @as(meta.FieldType(Header, .msg_len), @truncate(msg_len)),
            .metadata_len = @as(meta.FieldType(Header, .metadata_len), @truncate(metadata_len)),
        };
    }

    pub fn fromMessage(message: []const u8) Header {
        // Create header from message payload using well known offset bounds
        const bytes = message[serde.HEADER_OFFSET_START..serde.HEADER_OFFSET_END];
        return Header.fromBytes(bytes);
    }
};

const Metadata = struct {
    transmitter: ?[]const u8 = null,
    receiver: ?[]const u8 = null,
    func_name: ?[]const u8 = null,
    durable: bool = false,

    pub fn initFromMetadataFromStr(self: *Metadata, metadata_str: []const u8) void {
        // metadata_str is in format: transmitter|receiver|func_name
        var iterator = std.mem.splitScalar(u8, metadata_str, SECTION_SEPARATOR);
        var ind: usize = 0;
        while (iterator.next()) |val| : (ind += 1) {
            switch (ind) {
                0 => self.transmitter = val,
                1 => self.receiver = val,
                2 => self.func_name = val,
                else => unreachable,
            }
        }
    }

    pub fn initMetadataFromFields(self: *Metadata, transmitter: []const u8, receiver: []const u8, func_name: []const u8) void {
        self.transmitter = transmitter;
        self.receiver = receiver;
        self.func_name = func_name;
    }

    pub fn concatenateParts(allocator: Allocator, transmitter: ?[]const u8, receiver: ?[]const u8, func_name: ?[]const u8) ![]const u8 {
        // Concatenate transmitter, receiver and func_name into one metadata string
        const metadata_slice: []const []const u8 = &[_][]const u8{
            transmitter orelse PLACEHOLDER,
            receiver orelse PLACEHOLDER,
            func_name orelse PLACEHOLDER,
        };
        return try std.mem.join(allocator, SECTION_SEPARATOR_SEQ, metadata_slice);
    }

    pub fn calcMetadataLen(transmitter: []const u8, receiver: []const u8, func_name: []const u8) usize {
        // Calculate metadata length
        return transmitter.len + receiver.len + func_name.len + SECTION_SEPARATOR_SEQ.len * 2;
    }
};

pub const Message = struct {
    header: Header,
    data: []u8,
    allocator: Allocator,
    metadata: Metadata = .{},

    pub fn format(self: Message, _: []const u8, _: fmt.FormatOptions, writer: anytype) !void {
        const h = self.header;
        return fmt.format(writer, "<Message: {d:0>5}> | {s} |", .{ h.uuid, @tagName(h.flag) });
    }

    pub fn create(allocator: Allocator, header: Header, data: []u8) !*Message {
        var self = try allocator.create(Message);
        self.* = .{
            .header = header,
            .data = data,
            .allocator = allocator,
        };
        self.metadata.initFromMetadataFromStr(self.getMetadata());
        return self;
    }

    pub fn fromRawMessage(allocator: Allocator, raw_message: *const RawMessage) !*Message {
        const header = Header.fromMessage(raw_message.data);
        return try Message.create(allocator, header, raw_message.data);
    }

    pub fn deinit(self: *Message) void {
        if (self.metadata.durable) return;
        self.allocator.free(self.data);
        self.allocator.destroy(self);
    }

    pub fn setDurable(self: *Message) void {
        self.metadata.durable = true;
    }

    pub fn undurableAndDeinit(self: *Message) void {
        self.metadata.durable = false;
        self.deinit();
    }

    fn msgEndPos(self: *Message) usize {
        // Message length including all parts (header, metadata and data)
        // [header][metadata][data]
        return serde.HEADER_SIZE + self.header.msg_len;
    }

    fn metadataEndPos(self: *Message) usize {
        // Only header + medata (without data)
        // [header][metadata][-]
        return serde.HEADER_OFFSET_END + self.header.metadata_len;
    }

    fn metadataStartPos(_: *Message) usize {
        return serde.HEADER_OFFSET_END;
    }

    fn dataEndPos(self: *Message) usize {
        return self.msgEndPos();
    }

    fn dataStartPosition(self: *Message) usize {
        return serde.HEADER_OFFSET_END + self.header.metadata_len;
    }

    fn metadataLen(self: *Message) usize {
        // Only metadata length
        // [-][metadata][-]
        return self.header.metadata_len;
    }

    pub fn getDataLen(self: *Message) usize {
        // Only data length
        // [-][-][data]
        return self.header.msg_len - self.header.metadata_len;
    }

    pub fn getData(self: *Message) []const u8 {
        // Get only data without header and metadata.
        return self.data[self.dataStartPosition()..self.dataEndPos()];
    }

    pub fn getMetadata(self: *Message) []const u8 {
        return self.data[self.metadataStartPos()..self.metadataEndPos()];
    }

    pub fn getUuid(self: *Message) meta.FieldType(Header, .uuid) {
        return self.header.uuid;
    }

    pub fn getFlag(self: *Message) MessageFlag {
        return self.header.flag;
    }

    pub fn getDecoder(self: *Message) MessageDecoder {
        return self.header.decoder;
    }

    pub fn getTransmitter(self: *Message) []const u8 {
        return self.metadata.transmitter.?;
    }

    pub fn getReceiver(self: *Message) []const u8 {
        return self.metadata.receiver.?;
    }

    pub fn getFuncName(self: *Message) []const u8 {
        return self.metadata.func_name.?;
    }

    pub fn getReturnResult(self: *Message) bool {
        return self.header.return_result;
    }

    pub fn getMsgParts(self: *Message) meta.FieldType(Header, .msg_parts) {
        return self.header.msg_parts;
    }

    pub fn setData(self: *Message, data: []const u8) !void {
        const new_msg_len: usize = data.len + self.header.metadata_len;
        if (new_msg_len > std.math.maxInt(meta.FieldType(Header, .msg_len))) return error.ExceedsMaxMessageSize;
        if (self.allocator.resize(self.data, new_msg_len + serde.HEADER_SIZE)) {
            // Optimal case when we can resize existing memory. We don't need to copy data to new location.
            // header and metadata are not changed, so we don't need to copy them.
            self.data = self.data.ptr[0 .. new_msg_len + serde.HEADER_SIZE];
            std.mem.copyForwards(u8, self.data[self.dataStartPosition()..], data);
        } else {
            // Fall back to allocating new memory and copying data to it.
            var new_data = try serde.concatenateSlicesAlloc(self.allocator, &[_][]const u8{ self.data[0..serde.HEADER_OFFSET_END], self.getMetadata(), data });
            self.allocator.free(self.data);
            self.data = new_data;
            // re-calculate metadata as it is pointed to wrong location
            self.metadata.initFromMetadataFromStr(self.getMetadata());
        }
        self.setMessageLen(@as(meta.FieldType(Header, .msg_len), @truncate(new_msg_len)));
    }

    pub fn writeData(self: *Message, comptime fmt_format: []const u8, args: anytype) !void {
        var data = try fmt.allocPrint(self.allocator, fmt_format, args);
        defer self.allocator.free(data);
        try self.setData(data);
    }

    pub fn writeErrorMessage(self: *Message, comptime fmt_format: []const u8, args: anytype) !void {
        // For setting errors outside of python
        try self.writeData(fmt_format, args);
        self.setDecoder(.RAW);
        self.setFlag(.ERROR);
    }

    fn setMetadata(self: *Message, transmitter: ?[]const u8, receiver: ?[]const u8, func_name: ?[]const u8) !void {
        const ptransmitter = transmitter orelse self.getTransmitter();
        const preceiver = receiver orelse self.getReceiver();
        const pfun_name = func_name orelse self.getFuncName();
        const new_metadata_len = Metadata.calcMetadataLen(ptransmitter, preceiver, pfun_name);
        const data = self.getData();
        const new_msg_len: usize = data.len + new_metadata_len;
        if (new_msg_len > std.math.maxInt(meta.FieldType(Header, .msg_len))) return error.ExceedsMaxMessageSize;
        var new_data = try serde.concatenateSlicesAlloc(self.allocator, &[_][]const u8{ self.data[0..serde.HEADER_OFFSET_END], ptransmitter, SECTION_SEPARATOR_SEQ, preceiver, SECTION_SEPARATOR_SEQ, pfun_name, data });
        self.allocator.free(self.data);
        self.data = new_data;
        self.setMessageLen(@as(meta.FieldType(Header, .msg_len), @truncate(new_msg_len)));
        self.setMetadataLen(@as(meta.FieldType(Header, .metadata_len), @truncate(new_metadata_len)));
        // re-calculate metadata as it is pointed to wrong location
        self.metadata.initFromMetadataFromStr(self.getMetadata());
    }

    pub fn setUuid(self: *Message, uuid: meta.FieldType(Header, .uuid)) void {
        self.header.uuid = uuid;
        self.header.setBits(@TypeOf(uuid), "uuid", uuid, self.data);
    }

    pub fn setFlag(self: *Message, flag: MessageFlag) void {
        self.header.flag = flag;
        var value = @intFromEnum(flag);
        self.header.setBits(std.meta.Tag(MessageFlag), "flag", value, self.data);
    }

    pub fn setDecoder(self: *Message, decoder: MessageDecoder) void {
        self.header.decoder = decoder;
        var value = @intFromEnum(decoder);
        const ftype = std.meta.Tag(MessageDecoder);
        self.header.setBits(ftype, "decoder", value, self.data);
    }

    pub fn setTransmitter(self: *Message, transmitter: []const u8) !void {
        try self.setMetadata(transmitter, null, null);
    }

    pub fn setReceiver(self: *Message, receiver: []const u8) !void {
        try self.setMetadata(null, receiver, null);
    }

    pub fn setFuncName(self: *Message, func_name: []const u8) !void {
        try self.setMetadata(null, null, func_name);
    }

    pub fn setMessageLen(self: *Message, msg_len: meta.FieldType(Header, .msg_len)) void {
        self.header.msg_len = msg_len;
        self.header.setBits(@TypeOf(msg_len), "msg_len", msg_len, self.data);
    }

    pub fn setMetadataLen(self: *Message, metadata_len: meta.FieldType(Header, .metadata_len)) void {
        self.header.metadata_len = metadata_len;
        self.header.setBits(@TypeOf(metadata_len), "metadata_len", metadata_len, self.data);
    }

    pub fn setMsgParts(self: *Message, msg_parts: meta.FieldType(Header, .msg_parts)) void {
        self.header.msg_parts = msg_parts;
        self.header.setBits(meta.FieldType(Header, .msg_parts), "msg_parts", self.header.msg_parts, self.data);
    }

    pub fn hasReceiver(self: *Message) bool {
        return !std.mem.eql(u8, self.getReceiver(), PLACEHOLDER);
    }

    pub fn hasTransmitter(self: *Message) bool {
        return !std.mem.eql(u8, self.getTransmitter(), PLACEHOLDER);
    }

    pub fn isError(self: *Message) bool {
        return self.header.flag == .ERROR;
    }

    pub fn isBytes(self: *Message) bool {
        return self.header.is_bytes;
    }

    pub fn sendTo(self: *Message, writer: anytype) !void {
        _ = try writer.write(self.data);
    }
};

pub const RawMessage = struct {
    data: []u8,

    pub fn create(allocator: Allocator, data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, transmitter: ?[]const u8, receiver: ?[]const u8, func_name: ?[]const u8) !RawMessage {
        // Create RawMessage from data and header. Caller is responsible for freeing memory used for data generation.
        const ptransmitter = transmitter orelse PLACEHOLDER;
        const preceiver = receiver orelse PLACEHOLDER;
        const pfun_name = func_name orelse PLACEHOLDER;
        const metadata_len = Metadata.calcMetadataLen(ptransmitter, preceiver, pfun_name);
        const msg_len = data.len + metadata_len;
        if (msg_len > std.math.maxInt(meta.FieldType(Header, .msg_len))) return error.ExceedsMaxMessageSize;
        const header: *const Header = &try Header.create(uuid, flag, decoder, is_bytes, return_result, msg_len, metadata_len);
        var backed_header_bytes: [serde.HEADER_SIZE]u8 = undefined;
        header.toBytes(&backed_header_bytes);
        const slices = &[_][]const u8{ &backed_header_bytes, ptransmitter, SECTION_SEPARATOR_SEQ, preceiver, SECTION_SEPARATOR_SEQ, pfun_name, data };
        return .{ .data = try serde.concatenateSlicesAlloc(allocator, slices) };
    }

    pub fn createMetadata(allocator: Allocator, transmitter: ?[]const u8, receiver: ?[]const u8, func_name: ?[]const u8) ![]const u8 {
        return try Metadata.concatenateParts(allocator, transmitter, receiver, func_name);
    }

    pub fn sendTo(self: *const RawMessage, writer: anytype) !void {
        _ = try writer.write(self.data);
    }
};

pub fn createHandshakeMessage(allocator: Allocator, memberdata: []serde.Handshake.MemberData, uuid: meta.FieldType(Header, .uuid), password: ?[]const u8, comptime hstype: []const u8) !*Message {
    // gonna be using arena allocator to free dynamically allocated memory for json related stuff
    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();
    var handshake = try serde.Handshake.create(arena.allocator(), memberdata, password, hstype);
    var handshake_data = try handshake.toJson(arena.allocator());
    var raw_message = try RawMessage.create(allocator, handshake_data, uuid, .HANDSHAKE, .JSON, false, false, null, null, null);
    return try Message.fromRawMessage(allocator, &raw_message);
}

pub fn createEventMessage(allocator: Allocator, uuid: meta.FieldType(Header, .uuid), connection_name: []const u8, comptime event_type: []const u8) !*Message {
    var event = serde.Event.create(event_type, connection_name);
    var event_data = try event.toJson(allocator);
    defer allocator.free(event_data);
    var raw_message = try RawMessage.create(allocator, event_data, uuid, .EVENTS, .JSON, false, false, null, null, null);
    return try Message.fromRawMessage(allocator, &raw_message);
}


test "header to int" {
    var header = try Header.create(7777, .REQUEST, .JSON, false, true, 1000, 25);
    var backed_int = header.toInt();
    try expect(backed_int == 0x8000320003e8901e61);
    var restored = Header.fromInt(backed_int);
    try std.testing.expectEqual(header, restored);
}

test "header to bytes" {
    var header = try Header.create(7777, .REQUEST, .JSON, false, true, 1000, 25);
    var backed_bytes: [serde.HEADER_SIZE]u8 = undefined;
    header.toBytes(&backed_bytes);
    var restored = Header.fromBytes(&backed_bytes);
    try std.testing.expectEqual(header, restored);
}

test "serde RawMessage" {
    var alloc = std.testing.allocator;
    const transmitter = "transmitter";
    const receiver = "receiver";
    const func_name = "func_name";

    const message = try RawMessage.create(alloc, "hello world", 1000, .REQUEST, .JSON, false, true, transmitter, receiver, func_name);
    defer alloc.free(message.data);

    const restored_header = Header.fromMessage(message.data);
    const expected_metadata_len = Metadata.calcMetadataLen(transmitter, receiver, func_name);
    const expected_msg_len = "hello world".len + expected_metadata_len;

    try expect(restored_header.uuid == 1000);
    try expect(restored_header.msg_len == expected_msg_len);
    try expect(restored_header.metadata_len == expected_metadata_len);
    try expect(restored_header.flag == .REQUEST);
    try expect(restored_header.decoder == .JSON);
    try expect(restored_header.return_result == true);
    try expect(restored_header.is_bytes == false);
}

test "serde big RawMessage" {
    var alloc = std.testing.allocator;
    const transmitter = "transmitter";
    const receiver = "receiver";
    const func_name = "func_name";
    var data: []const u8 = "friedeggs" ** 11e5;

    const message = try RawMessage.create(alloc, data, 1000, .REQUEST, .JSON, false, true, transmitter, receiver, func_name);
    defer alloc.free(message.data);

    const restored_header = Header.fromMessage(message.data);
    const expected_metadata_len = Metadata.calcMetadataLen(transmitter, receiver, func_name);
    const expected_msg_len = data.len + expected_metadata_len;

    try expect(restored_header.uuid == 1000);
    try expect(restored_header.msg_len == expected_msg_len);
    try expect(restored_header.metadata_len == expected_metadata_len);
    try expect(restored_header.flag == .REQUEST);
    try expect(restored_header.decoder == .JSON);
    try expect(restored_header.return_result == true);
    try expect(restored_header.is_bytes == false);
}

test "serdeMessage create metadata" {
    const metadata = try RawMessage.createMetadata(std.testing.allocator, "transmitter", "receiver", "func_name");
    defer std.testing.allocator.free(metadata);
    try expect(std.mem.eql(u8, metadata, "transmitter|receiver|func_name"));
}

test "Message set fields" {
    var alloc = std.testing.allocator;
    const transmitter = "transmitter";
    const receiver = "receiver";
    const func_name = "func_name";
    const data = &[_]u8{'1'} ** 100;

    const metadata_len = Metadata.calcMetadataLen(transmitter, receiver, func_name);
    const msg_len = data.len + metadata_len;
    var header = try Header.create(7777, .REQUEST, .JSON, false, true, msg_len, metadata_len);
    var backed_header_bytes: [serde.HEADER_SIZE]u8 = undefined;
    header.toBytes(&backed_header_bytes);

    var raw_message = try RawMessage.create(alloc, data, 1000, .REQUEST, .JSON, false, true, transmitter, receiver, func_name);
    var msg = try Message.create(alloc, header, raw_message.data);
    defer msg.deinit();

    try expect(std.mem.eql(u8, msg.getData(), data));
    try expect(std.mem.eql(u8, msg.getMetadata(), "transmitter|receiver|func_name"));
    try expect(std.mem.eql(u8, msg.getTransmitter(), transmitter));
    try expect(std.mem.eql(u8, msg.getReceiver(), receiver));
    try expect(std.mem.eql(u8, msg.getFuncName(), func_name));

    try msg.setData("22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222");
    try expect(std.mem.eql(u8, msg.getData(), "22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222"));
    try expect(std.mem.eql(u8, msg.getMetadata(), "transmitter|receiver|func_name"));
    try expect(std.mem.eql(u8, msg.getTransmitter(), transmitter));
    try expect(std.mem.eql(u8, msg.getReceiver(), receiver));
    try expect(std.mem.eql(u8, msg.getFuncName(), func_name));

    try msg.setData("new data");
    try expect(std.mem.eql(u8, msg.getData(), "new data"));
    try expect(std.mem.eql(u8, msg.getMetadata(), "transmitter|receiver|func_name"));
    try expect(std.mem.eql(u8, msg.getTransmitter(), transmitter));
    try expect(std.mem.eql(u8, msg.getReceiver(), receiver));
    try expect(std.mem.eql(u8, msg.getFuncName(), func_name));

    try expect(msg.getUuid() == 7777);
    msg.setUuid(8888);
    try expect(msg.getUuid() == 8888);

    try msg.setTransmitter("new transmitter");
    try expect(std.mem.eql(u8, msg.getTransmitter(), "new transmitter"));
    try expect(std.mem.eql(u8, msg.getReceiver(), receiver));
    try expect(std.mem.eql(u8, msg.getFuncName(), func_name));
    try expect(std.mem.eql(u8, msg.getData(), "new data"));

    try msg.setReceiver("new receiver");
    try expect(std.mem.eql(u8, msg.getTransmitter(), "new transmitter"));
    try expect(std.mem.eql(u8, msg.getReceiver(), "new receiver"));
    try expect(std.mem.eql(u8, msg.getFuncName(), func_name));
    try expect(std.mem.eql(u8, msg.getData(), "new data"));

    try msg.setFuncName("new func name");
    try expect(std.mem.eql(u8, msg.getTransmitter(), "new transmitter"));
    try expect(std.mem.eql(u8, msg.getReceiver(), "new receiver"));
    try expect(std.mem.eql(u8, msg.getFuncName(), "new func name"));
    try expect(std.mem.eql(u8, msg.getData(), "new data"));

    msg.setFlag(.REQUEST);
    try expect(msg.getFlag() == .REQUEST);

    msg.setDecoder(.PICKLE);
    try expect(msg.getDecoder() == .PICKLE);
}

test "Message has transmitter/receiver" {
    var alloc = std.testing.allocator;
    const transmitter = "";
    const receiver = "";
    const func_name = "func_name";
    const data = &[_]u8{'1'} ** 100;

    const metadata_len = Metadata.calcMetadataLen(transmitter, receiver, func_name);
    const msg_len = data.len + metadata_len;
    var header = try Header.create(7777, .REQUEST, .JSON, false, true, msg_len, metadata_len);
    var backed_header_bytes: [serde.HEADER_SIZE]u8 = undefined;
    header.toBytes(&backed_header_bytes);

    var raw_message = try RawMessage.create(alloc, data, 1000, .REQUEST, .JSON, false, true, transmitter, receiver, func_name);
    var msg = try Message.create(alloc, header, raw_message.data);
    defer msg.deinit();
    try expect(!msg.hasReceiver());
    try expect(!msg.hasTransmitter());
}
