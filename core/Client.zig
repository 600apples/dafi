const std = @import("std");
const builtin = @import("builtin");
const net = std.net;
const os = std.os;
const network = @import("network.zig");
const handlers = @import("handlers.zig");
const serde = @import("serde.zig");
const repository = @import("repository.zig");

const assert = std.debug.assert;
const MessageFlag = serde.MessageFlag;
const Message = serde.Message;
const Handshake = serde.Handshake;
const MessagePool = serde.MessagePool;
const MessageDecoder = serde.MessageDecoder;
const ClientConnection = network.Connection(.ClientConnectionType);
const ClientHandler = handlers.ClientHandler;
const MessageHandler = handlers.MessageHandler;
const ClientMessageStore = repository.ClientMessageStore;
const PLACEHOLDER = serde.PLACEHOLDER;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const print = @import("misc.zig").print;
const is_wasm = @import("misc.zig").is_wasm;

const Client = @This();
var clientEntries: ?std.BoundedArray(?*ClientEntry, 256) = null;

fn setSignalHandler() !void {
    const internal_handler = struct {
        fn internal_handler(_: c_int) callconv(.C) void {
            std.debug.print("Exiting...\n", .{});
            os.exit(0);
        }
    }.internal_handler;
    const act = os.Sigaction{
        .handler = .{ .handler = internal_handler },
        .mask = os.empty_sigset,
        .flags = 0,
    };
    try os.sigaction(os.SIG.INT, &act, null);
    try os.sigaction(os.SIG.TERM, &act, null);
}

pub const MessageIdentifier = struct {
    uuid: u16,
    timestamp: i64,
    receiver: []const u8,
};

pub const MessageAndDataIndentifier = struct {
    message: MessageIdentifier,
    data: []const u8,
};

const ClientEntry = struct {
    connection: *ClientConnection,
    msgpool: *MessagePool,
    msg_store: ClientMessageStore,
    client_handler: *ClientHandler,
    uuid: u16 = 0,

    pub fn get(conn_num: usize) !*ClientEntry {
        if (clientEntries) |entries| {
            if (entries.get(conn_num)) |client_entry| {
                if (!client_entry.connection.suspended) return client_entry;
                return error.ConnectionLost;
            }
        }
        return error.ClientNotInitialized;
    }

    pub fn generateUUID(self: *ClientEntry) u16 {
        const ov = @addWithOverflow(self.uuid, 1);
        if (ov[1] != 0) self.uuid = 1 // skip 0
        else self.uuid = ov[0];
        return self.uuid;
    }

    pub fn desctroyClientEntry(self: *const ClientEntry) void {
        self.connection.close();
        self.msgpool.deinit();
    }

    fn messageDispatcherClientEntry(self: *ClientEntry, conn_num: usize) !void {
        // TODO: close connection?
        var msg_handler = self.client_handler.handler();
        while (self.msgpool.receiveMessage(self.connection)) |message| {
            try msg_handler.handle(self.connection, message);
        } else |err| {
            print("exit client connection with error: {}\n", .{err});
            try msg_handler.handleErr(self.connection, err);
        }
        try msg_handler.handleDisconnect(self.connection);
        std.os.nanosleep(0, 5000);
        if (clientEntries) |*entries| entries.set(conn_num, null);
    }

    pub fn createMessageClientEntry(self: *ClientEntry, allocator: Allocator, data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, receiver: []const u8, func_name: []const u8) !MessageAndDataIndentifier {
        const ts = if (is_wasm) 0 else std.time.timestamp();
        if (flag == .REQUEST) {
            // Find intersection between provided receiver and available receivers
            const actual_receiver = try self.client_handler.findReceiverForMethod(func_name, if (!std.mem.eql(u8, receiver, "")) receiver else null);
            const actual_uuid = self.generateUUID();
            var msg = try self.msgpool.createMessage(allocator, data, actual_uuid, flag, decoder, is_bytes, return_result, self.client_handler.app_name, actual_receiver, func_name);
            return .{ .message = .{ .receiver = actual_receiver, .uuid = actual_uuid, .timestamp = ts }, .data = msg };
        } else {
            std.debug.assert(uuid != 0);
            var msg = try self.msgpool.createMessage(allocator, data, uuid, flag, decoder, is_bytes, return_result, self.client_handler.app_name, receiver, func_name);
            return .{ .message = .{ .receiver = receiver, .uuid = uuid, .timestamp = ts }, .data = msg };
        }
    }

    pub fn sendMessageClientEntry(self: *ClientEntry, data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, receiver: []const u8, func_name: []const u8) !MessageIdentifier {
        if (flag == .REQUEST) {
            // Find intersection between provided receiver and available receivers
            const actual_receiver = try self.client_handler.findReceiverForMethod(func_name, if (!std.mem.eql(u8, receiver, "")) receiver else null);
            const actual_uuid = self.generateUUID();
            try self.msgpool.sendMessage(self.connection, data, actual_uuid, flag, decoder, is_bytes, return_result, self.client_handler.app_name, actual_receiver, func_name);
            return .{ .receiver = actual_receiver, .uuid = actual_uuid, .timestamp = std.time.timestamp() };
        } else {
            std.debug.assert(uuid != 0);
            try self.msgpool.sendMessage(self.connection, data, uuid, flag, decoder, is_bytes, return_result, self.client_handler.app_name, receiver, func_name);
            return .{ .receiver = receiver, .uuid = uuid, .timestamp = std.time.timestamp() };
        }
    }

    pub fn createHandshakeClientEntry(self: *ClientEntry, allocator: Allocator, password: []const u8, methods: []const u8) !MessageAndDataIndentifier {
        try self.client_handler.setClientMethods(methods);
        var handshake = try Handshake.create(allocator, &[_]Handshake.MemberData{.{ .name = self.client_handler.app_name, .methods = methods }}, password, "client");
        var data = try handshake.toJson(allocator);
        const uuid = self.generateUUID();
        var msg = try self.msgpool.createMessage(allocator, data, uuid, .HANDSHAKE, .JSON, false, true, self.client_handler.app_name, PLACEHOLDER, PLACEHOLDER);
        const ts = if (is_wasm) 0 else std.time.timestamp();
        return .{ .message = .{ .receiver = PLACEHOLDER, .uuid = uuid, .timestamp = ts }, .data = msg };
    }

    pub fn sendHandshakeClientEntry(self: *ClientEntry, password: []const u8, methods: []const u8) !MessageIdentifier {
        var arena = ArenaAllocator.init(std.heap.c_allocator);
        defer arena.deinit();
        var allocator = arena.allocator();
        try self.client_handler.setClientMethods(methods);
        var handshake = try Handshake.create(allocator, &[_]Handshake.MemberData{.{ .name = self.client_handler.app_name, .methods = methods }}, password, "client");
        var data = try handshake.toJson(allocator);
        const uuid = self.generateUUID();
        try self.msgpool.sendMessage(self.connection, data, uuid, .HANDSHAKE, .JSON, false, true, self.client_handler.app_name, PLACEHOLDER, PLACEHOLDER);
        return .{
            .receiver = PLACEHOLDER,
            .uuid = uuid,
            .timestamp = std.time.timestamp(),
        };
    }

    pub fn getMessageByUuidClientEntry(self: *ClientEntry, uuid: u16) ?*Message {
        return self.msg_store.fetch(uuid);
    }

    pub fn setTimeoutErrorClientEntry(self: *ClientEntry, uuid: u16) !void {
        try self.msg_store.setTimeoutError(uuid);
    }

    pub fn getMessageForClientWorkerClientEntry(self: *ClientEntry) ?*Message {
        return self.client_handler.tasks_queue.getMessageFromQueue();
    }

    pub fn getAvailableMembersClientEntry(self: *ClientEntry, allocator: Allocator) ![]const u8 {
        const chan_count = self.client_handler.chan_mapper.ChannelsCount();
        if (chan_count == 0) return PLACEHOLDER;
        var arena = ArenaAllocator.init(allocator); // page allocator to support webassembly
        defer arena.deinit();
        const this_name = try std.fmt.allocPrint(arena.allocator(), "{s} (this app)", .{self.client_handler.app_name});
        var memberdata = try arena.allocator().alloc(Handshake.MemberData, chan_count);
        for (self.client_handler.chan_mapper.allChannels(), 0..) |*c, idx| {
            const name = if (std.mem.eql(u8, c.connection_name, self.client_handler.app_name)) this_name else c.connection_name;
            memberdata[idx] = .{ .name = name, .methods = try c.joinedMethods(arena.allocator(), serde.UNIT_SEPARATOR) };
        }
        var handshake = try Handshake.create(arena.allocator(), memberdata, null, null);
        return try handshake.toJson(arena.allocator());
    }
};

fn messageDispatcher(conn_num: usize) !void {
    var entry = try ClientEntry.get(conn_num);
    try entry.messageDispatcherClientEntry(conn_num);
}

pub fn init(allocator: std.mem.Allocator, app_name: []const u8, config: ClientConnection.Config) !usize {
    var conn = try ClientConnection.init(allocator, config);
    var msgpool = try MessagePool.init(allocator);
    var client_handler = try ClientHandler.init(allocator, app_name);
    var msg_store = client_handler.msg_store;
    if (!is_wasm) {
        try setSignalHandler();
        try msgpool.sendSynMessage(conn);
    }
    var cl_entry = try allocator.create(ClientEntry);
    cl_entry.* = ClientEntry{ .connection = conn, .msgpool = msgpool, .client_handler = client_handler, .msg_store = msg_store };
    if (clientEntries == null) {
        clientEntries = try std.BoundedArray(?*ClientEntry, 256).init(256);
    }
    const conn_num: usize = @rem(clientEntries.?.len, 256);
    clientEntries.?.set(conn_num, cl_entry);
    if (!is_wasm) _ = try std.Thread.spawn(.{}, messageDispatcher, .{conn_num});
    return conn_num;
}

pub fn desctroyClient(conn_num: usize) !void {
    var entry = try ClientEntry.get(conn_num);
    entry.desctroyClientEntry();
}

pub fn createMessage(allocator: Allocator, data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, receiver: []const u8, func_name: []const u8, conn_num: usize) !MessageAndDataIndentifier {
    var entry = try ClientEntry.get(conn_num);
    return try entry.createMessageClientEntry(allocator, data, uuid, flag, decoder, is_bytes, return_result, receiver, func_name);
}

pub fn sendMessage(data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, receiver: []const u8, func_name: []const u8, conn_num: usize) !MessageIdentifier {
    var entry = try ClientEntry.get(conn_num);
    return entry.sendMessageClientEntry(data, uuid, flag, decoder, is_bytes, return_result, receiver, func_name);
}

pub fn createHandshake(allocator: Allocator, password: []const u8, methods: []const u8, conn_num: usize) !MessageAndDataIndentifier {
    var entry = try ClientEntry.get(conn_num);
    return try entry.createHandshakeClientEntry(allocator, password, methods);
}

pub fn sendHandshake(conn_num: usize, password: []const u8, methods: []const u8) !MessageIdentifier {
    var entry = try ClientEntry.get(conn_num);
    return try entry.sendHandshakeClientEntry(password, methods);
}

pub fn getMessageByUuid(uuid: u16, conn_num: usize) !?*Message {
    var entry = try ClientEntry.get(conn_num);
    return entry.getMessageByUuidClientEntry(uuid);
}

pub fn setTimeoutError(uuid: u16, conn_num: usize) !void {
    var entry = try ClientEntry.get(conn_num);
    try entry.setTimeoutErrorClientEntry(uuid);
}

pub fn getMessageForClientWorker(conn_num: usize) !?*Message {
    var entry = ClientEntry.get(conn_num) catch return null;
    return entry.getMessageForClientWorkerClientEntry();
}

pub fn getAvailableMembers(allocator: Allocator, conn_num: usize) ![]const u8 {
    var entry = try ClientEntry.get(conn_num);
    return entry.getAvailableMembersClientEntry(allocator);
}

pub fn getClientEntry(conn_num: usize) !*ClientEntry {
    // Handle message outside of client connection loop (for wasm)
    return try ClientEntry.get(conn_num);
}
