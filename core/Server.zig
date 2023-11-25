const std = @import("std");
const web = @import("web.zig");
const serde = @import("serde.zig");
const network = @import("network.zig");
const handlers = @import("handlers.zig");
const repository = @import("repository.zig");
const os = std.os;

const Allocator = std.mem.Allocator;
const ServerConnection = network.Connection(.ServerConnectionType);
const ServerHandler = handlers.ServerHandler(.ClientConnectionType);
const MessageHandler = handlers.MessageHandler;
const Message = serde.Message;
const MessageFlag = serde.MessageFlag;
const MessagePool = serde.MessagePool;
const MessageDecoder = serde.MessageDecoder;

var serverEntries: ?std.BoundedArray(ServerEntry, 256) = null;

fn setSignalHandler() !void {
    const internal_handler = struct {
        fn internal_handler(_: c_int) callconv(.C) void {
            if (serverEntries) |*entries| {
                while (entries.popOrNull()) |entry| {
                    entry.connection.close();
                }
            }
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

const ServerEntry = struct {
    connection: *ServerConnection,
    msgpool: *MessagePool,
    server_handler: *ServerHandler,

    pub fn sendMessageServerEntry(self: ServerEntry, data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, receiver: []const u8, func_name: []const u8) !MessageIdentifier {
        // TODO: send messages to WebMessagePool
        switch (self.server_handler.*) {
            .Service => |*s| {
                std.debug.assert(uuid != 0);
                var chan = try s.chan_mapper.getChannel(receiver);
                try self.msgpool.sendMessage(chan.conn, data, uuid, flag, decoder, is_bytes, return_result, s.app_name, receiver, func_name);
                return .{ .receiver = receiver, .uuid = uuid, .timestamp = std.time.timestamp() };
            },
            .Controller => return error.MessagesNotSupported,
        }
    }

    pub fn getMessageForServerWorkerServerEntry(self: ServerEntry) !?*Message {
        switch (self.server_handler.*) {
            .Service => |*s| return s.tasks_queue.getMessageFromQueue(),
            .Controller => return error.QueueNotSupported,
        }
    }

    pub fn setServiceMethodsServerEntry(self: ServerEntry, methods: []const u8) !void {
        try switch (self.server_handler.*) {
            .Service => |*s| s.setServiceMethods(methods),
            .Controller => error.MethodsNotSupported,
        };
    }
};

pub fn messageDispatcher(allocator: Allocator, app_name: []const u8, config: ServerConnection.Config, conn_num: usize) !void {
    // Entry point for server. It creates connection and message pool and starts server loop.
    try setSignalHandler();
    var msgpool: *MessagePool = try MessagePool.init(allocator);
    const conn: *ServerConnection = try ServerConnection.init(allocator, config);
    var server_handler = try ServerHandler.init(config.mode, allocator, app_name);
    if (serverEntries == null) {
        serverEntries = try std.BoundedArray(ServerEntry, 256).init(0);
    }
    var server_entry = ServerEntry{ .connection = conn, .msgpool = msgpool, .server_handler = server_handler };
    serverEntries.?.set(conn_num, server_entry);
    // Conection normally should be closed by destroyConnection method.
    // msgpool.deinit should happen after connection is closed.
    defer msgpool.deinit();
    while (!conn.suspended) {
        if (conn.accept()) |client_conn| {
            var th = if (client_conn.is_websocket)
                switch (config.mode) {
                    .Service => try std.Thread.spawn(.{}, serverWsLoop, .{ client_conn, msgpool, &server_handler.Service.handler() }),
                    .Controller => try std.Thread.spawn(.{}, serverWsLoop, .{ client_conn, msgpool, &server_handler.Controller.handler() }),
                }
            else switch (config.mode) {
                .Service => try std.Thread.spawn(.{}, serverLoop, .{ client_conn, msgpool, &server_handler.Service.handler() }),
                .Controller => try std.Thread.spawn(.{}, serverLoop, .{ client_conn, msgpool, &server_handler.Controller.handler() }),
            };
            th.detach();
        } else |err| {
            std.debug.print("failed to accept connection {}\n", .{err});
        }
    }
}

fn serverLoop(conn: *ServerHandler.ConnectionT, msgpool: *MessagePool, msg_handler: anytype) !void {
    defer conn.close();
    while (msgpool.receiveMessage(conn)) |message| {
        defer message.deinit();
        try msg_handler.handle(conn, message);
    } else |err| try msg_handler.handleErr(conn, err);
    try msg_handler.handleDisconnect(conn);
}

fn serverWsLoop(conn: *ServerHandler.ConnectionT, msgpool: *MessagePool, msg_handler: anytype) !void {
    defer conn.close();
    while (msgpool.receiveWsMessage(conn)) |maybe_message| {
        var message = maybe_message orelse break;
        defer message.deinit();
        try msg_handler.handle(conn, message);
    } else |err| try msg_handler.handleErr(conn, err);
    try msg_handler.handleDisconnect(conn);
}

pub fn sendMessage(data: []const u8, uuid: u16, flag: MessageFlag, decoder: MessageDecoder, is_bytes: bool, return_result: bool, receiver: []const u8, func_name: []const u8, conn_num: usize) !MessageIdentifier {
    if (serverEntries) |entries| return try entries.get(conn_num).sendMessageServerEntry(data, uuid, flag, decoder, is_bytes, return_result, receiver, func_name);
    return error.ServerNotInitialized;
}

pub fn destroyHandler(conn_num: usize) void {
    if (serverEntries) |entries| entries.get(conn_num).connection.close();
}

pub fn getMessageForServerWorker(conn_num: usize) !?*Message {
    if (serverEntries) |entries| return try entries.get(conn_num).getMessageForServerWorkerServerEntry();
    return error.ServerNotInitialized;
}

pub fn setServiceMethods(methods: []const u8, conn_num: usize) !void {
    if (serverEntries) |entries| try entries.get(conn_num).setServiceMethodsServerEntry(methods);
    return error.ServerNotInitialized;
}
