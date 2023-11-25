const std = @import("std");
const mem = std.mem;
const fifo = std.fifo;
const net = std.net;
const handlers = @import("../handlers.zig");
const serde = @import("../serde.zig");
const network = @import("../network.zig");
const repository = @import("../repository.zig");
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const Message = serde.Message;
const RawMessage = serde.RawMessage;
const Handshake = serde.Handshake;
const MessageFlag = serde.MessageFlag;
const ConnectionType = network.ConnectionType;
const Connection = network.Connection;
const MessagePool = serde.MessagePool;
const MessageHandler = handlers.MessageHandler;
const ClientMessageStore = repository.ClientMessageStore;
const ChannelsMapper = repository.ChannelsMapper;
const TasksQueue = repository.TasksQueue;
const print = @import("../misc.zig").print;

pub const HandlerMode = enum {
    Service,
    Controller,
};

fn RoundRobinIterator(comptime T: type) type {
    return struct {
        position: usize = 0,
        counter: usize = 0,

        fn next(self: *@This(), data: []const T) ?T {
            const data_len = data.len;
            if (data_len == 0) return null;
            while (self.counter < data_len) {
                // round-robin task cycle. start from end position of previous call.
                self.counter += 1;
                self.position = @rem(self.position + 1, data_len);
                return data[self.position];
            }
            return null;
        }

        fn reset(self: *@This()) void {
            self.counter = 0;
        }
    };
}

const CommonHandlers = struct {
    pub fn onEmpty(ctx: *anyopaque, message: *Message) !void {
        _ = ctx;
        // return error.NoHandler;
        print("No handler for message: {}\n", .{message.getUuid()});
    }
};

pub fn ServerHandler(comptime HandlerConnectionT: ConnectionType) type {

    // Service/Controller handlers
    return union(HandlerMode) {
        Service: Service,
        Controller: Controller,

        const HandlerUnion = @This();
        pub const ConnectionT = Connection(HandlerConnectionT);
        const ChannelsMapperT = ChannelsMapper(ConnectionT);

        pub fn init(mode: HandlerMode, allocator: Allocator, app_name: []const u8) !*HandlerUnion {
            var self = try allocator.create(HandlerUnion);
            return switch (mode) {
                .Service => blk: {
                    self.* = .{ .Service = .{ .allocator = allocator, .chan_mapper = try ChannelsMapperT.init(allocator), .tasks_queue = try TasksQueue.init(allocator), .app_name = app_name } };
                    break :blk self;
                },
                .Controller => blk: {
                    self.* = .{ .Controller = .{ .allocator = allocator, .chan_mapper = try ChannelsMapperT.init(allocator), .app_name = app_name } };
                    break :blk self;
                },
            };
        }

        pub fn deinit(self: *HandlerUnion) void {
            switch (self) {
                inline else => |e| e.allocator.destroy(e),
            }
        }

        const Service = struct {
            chan_mapper: *ChannelsMapperT,
            tasks_queue: TasksQueue,
            allocator: Allocator,
            app_name: []const u8,
            methods: ?[]const u8 = null, // comma separated list of service methods
            task_cycle_position: usize = 0,
            mutex: std.Thread.Mutex = .{},
            chan_iterator: RoundRobinIterator(ChannelsMapperT.Channel) = .{},

            const Self = @This();
            pub const ParentConnT = ConnectionT;
            const MessageHandlerT = MessageHandler(Self);

            pub fn handler(self: *const Self) MessageHandlerT {
                return .{
                    .ptr = @constCast(self),
                    .handlers_mapping = MessageHandlerT.createMapping(.{ .HANDSHAKE = onHandshake, .REQUEST = onRequest }),
                    .error_handler = errorHandler,
                    .disconnection_handler = diconnectionHandler,
                };
            }

            pub fn setServiceMethods(self: *Self, methods: []const u8) !void {
                if (self.methods) |m| self.allocator.free(m);
                self.methods = try self.allocator.dupe(u8, methods);
            }

            fn sendToConnection(_: *Self, comptime MessageT: type, conn: *ParentConnT, message: *MessageT) !void {
                message.sendTo(conn) catch |err| {
                    switch (err) {
                        error.NotOpenForWriting, error.BrokenPipe => print("error while sending message\n", .{}),
                        else => print("failed to send message to {any}: {any}\n", .{ conn.getAddr().?, err }),
                    }
                };
            }

            // ----------------------------- SERVICE HANDLERS -----------------------------
            fn onHandshake(self: *Self, conn: *ParentConnT, message: *Message) !void {
                self.mutex.lock();
                defer self.mutex.unlock();
                var arena = ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                var allocator = arena.allocator();
                var client_hs = Handshake.fromJson(allocator, message.getData()) catch |err| {
                    try message.writeErrorMessage("failed to parse handshake message: {s}", .{@errorName(err)});
                    try self.sendToConnection(Message, conn, message);
                    return;
                };
                defer client_hs.deinit();
                // TODO: check password
                const connection_name = try allocator.dupe(u8, message.getTransmitter());
                _ = try self.chan_mapper.getOrCreateChannel(conn, connection_name);
                var methods = self.methods orelse serde.PLACEHOLDER;

                var service_hs = try Handshake.create(allocator, &[_]Handshake.MemberData{.{ .name = self.app_name, .methods = methods }}, null, "service");
                var data = try service_hs.toJson(allocator);
                try message.setData(data);
                for (self.chan_mapper.channels.values()) |c| try self.sendToConnection(Message, c.conn, message);

                // Store event message for service itself.
                var event_message = try serde.createEventMessage(self.allocator, 0, connection_name, "connected");
                try self.tasks_queue.pushMessageToQueue(event_message);
            }

            fn onRequest(self: *Self, _: *ParentConnT, message: *Message) !void {
                // put message to task queue for processing by service worker.
                self.mutex.lock();
                defer self.mutex.unlock();
                message.setDurable();
                try self.tasks_queue.pushMessageToQueue(message);
            }

            // ---------------------- SERVICE CONNECTION LIFECYCLE ------------------------
            fn diconnectionHandler(self: *Self, conn: *ParentConnT) !void {
                self.mutex.lock();
                defer self.mutex.unlock();
                var arena = ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                var allocator = arena.allocator();

                var chan = self.chan_mapper.getChannelByConnection(conn) orelse return error.ChannelNotFound;
                var connection_name = try allocator.dupe(u8, chan.connection_name); // for event
                self.chan_mapper.destroyChannel(chan);

                // Store event message for service itself.
                var event_message = try serde.createEventMessage(self.allocator, 0, connection_name, "disconnected");
                try self.tasks_queue.pushMessageToQueue(event_message);
            }

            fn errorHandler(_: *Self, conn: *ParentConnT, err: anyerror) !void {
                const addr = conn.getAddr() orelse null;
                switch (err) {
                    error.EOF => print("connection closed by peer: {any}\n", .{addr}),
                    error.IncompleteMessage => print("incomplete message from {any}\n", .{addr}),
                    else => print("failed to receive message from none: {any}\n", .{err}),
                }
            }
        };

        const Controller = struct {
            chan_mapper: *ChannelsMapperT,
            app_name: []const u8,
            allocator: Allocator,

            const Self = @This();
            pub const ParentConnT = ConnectionT;
            const MessageHandlerT = MessageHandler(Self);
            pub fn handler(self: *const Self) MessageHandlerT {
                return .{
                    .ptr = @constCast(self),
                    .handlers_mapping = MessageHandlerT.createMapping(.{ .HANDSHAKE = onHandshake, .REQUEST = onRequest, .RESPONSE = onResponse, .ERROR = onResponse }),
                    .error_handler = errorHandler,
                    .disconnection_handler = diconnectionHandler,
                };
            }

            fn findChannel(self: *Self, method: ?[]const u8, receiver: []const u8) ?ChannelsMapperT.Channel {
                var chan = self.chan_mapper.getChannel(receiver) catch {
                    print("Something went wrong. No channel for receiver: {s}\n", .{receiver});
                    return null;
                };
                if (method == null or chan.containsMethod(method.?)) return chan;
                return null;
            }

            fn sendToConnection(_: *Self, comptime MessageT: type, conn: *ParentConnT, message: *MessageT) !void {
                message.sendTo(conn) catch |err| {
                    switch (err) {
                        error.NotOpenForWriting, error.BrokenPipe => print("error while sending message\n", .{}),
                        else => print("failed to send message to {any}: {any}\n", .{ conn.getAddr().?, err }),
                    }
                };
            }

            // ----------------------------- ROUTER HANDLERS -----------------------------
            fn onHandshake(self: *Self, conn: *ParentConnT, message: *Message) !void {
                var arena = ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                var allocator = arena.allocator();
                const connection_name = try allocator.dupe(u8, message.getTransmitter()); // for event
                var client_hs = Handshake.fromJson(allocator, message.getData()) catch |err| {
                    try message.writeErrorMessage("failed to parse handshake message: {s}", .{@errorName(err)});
                    try self.sendToConnection(Message, conn, message);
                    return;
                };
                for (client_hs.value.members) |mb| {
                    var chan = try self.chan_mapper.getOrCreateChannel(conn, mb.name);
                    chan.clear();
                    if (mb.methods) |methods| {
                        for (methods) |mt| try chan.addMethod(mt);
                    }
                }
                const chan_count = self.chan_mapper.ChannelsCount();
                var memberdata = try allocator.alloc(Handshake.MemberData, chan_count);
                for (self.chan_mapper.allChannels(), 0..) |*c, idx| {
                    memberdata[idx] = .{ .name = c.connection_name, .methods = try c.joinedMethods(allocator, serde.UNIT_SEPARATOR) };
                }
                var router_hs = try Handshake.create(allocator, memberdata, null, "router");
                var data = try router_hs.toJson(allocator);
                try message.setData(data);
                for (self.chan_mapper.channels.values()) |c| try self.sendToConnection(Message, c.conn, message);

                // Event for all channels.
                var event_message = try serde.createEventMessage(self.allocator, 0, connection_name, "connected");
                defer event_message.deinit();
                for (self.chan_mapper.channels.values()) |c| {
                    // Send event to all channels except connected.
                    if (!std.mem.eql(u8, connection_name, c.connection_name)) try self.sendToConnection(Message, c.conn, event_message);
                }
            }

            fn onRequest(self: *Self, _: *ParentConnT, message: *Message) !void {
                if (self.findChannel(message.getFuncName(), message.getReceiver())) |c| try self.sendToConnection(Message, c.conn, message) else print("No connections found for message: {any}\n", .{message});
            }

            fn onResponse(self: *Self, _: *ParentConnT, message: *Message) !void {
                if (self.findChannel(null, message.getReceiver())) |*c| try self.sendToConnection(Message, c.conn, message) else print("No connections found for message: {any}\n", .{message});
            }

            // --------------------- ROUTER CONNECTION LIFECYCLE ----------------------
            fn diconnectionHandler(self: *Self, conn: *ParentConnT) !void {
                var arena = ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                var allocator = arena.allocator();

                var chan = self.chan_mapper.getChannelByConnection(conn) orelse return error.ChannelNotFound;
                var connection_name = try allocator.dupe(u8, chan.connection_name); // for event
                self.chan_mapper.destroyChannel(chan);
                // Updated handshake message for all channels.
                const chan_count = self.chan_mapper.ChannelsCount();
                var memberdata = try allocator.alloc(Handshake.MemberData, chan_count);
                for (self.chan_mapper.allChannels(), 0..) |*c, idx| {
                    memberdata[idx] = .{ .name = c.connection_name, .methods = try c.joinedMethods(allocator, serde.UNIT_SEPARATOR) };
                }
                var handshake_message = try serde.createHandshakeMessage(self.allocator, memberdata, 0, null, "router");
                defer handshake_message.deinit();
                for (self.chan_mapper.channels.values()) |c| try self.sendToConnection(Message, c.conn, handshake_message);

                // Event for all channels.
                var event_message = try serde.createEventMessage(self.allocator, 0, connection_name, "disconnected");
                defer event_message.deinit();
                for (self.chan_mapper.channels.values()) |c| try self.sendToConnection(Message, c.conn, event_message);
            }

            fn errorHandler(_: *Self, conn: *ParentConnT, err: anyerror) !void {
                const addr = conn.getAddr() orelse null;
                switch (err) {
                    error.EOF => print("connection closed by peer: {any}\n", .{addr}),
                    error.IncompleteMessage => print("incomplete message from {any}\n", .{addr}),
                    else => print("failed to receive message from none: {any}\n", .{err}),
                }
            }
        };
    };
}

// Client handlers

pub const ClientHandler = struct {
    msg_store: ClientMessageStore,
    tasks_queue: TasksQueue,
    chan_mapper: *ChannelsMapperT,
    app_name: []const u8,
    methods: ?[]const u8 = null, // comma separated list of service methods
    allocator: Allocator,
    mutex: std.Thread.Mutex = .{},
    chan_iterator: RoundRobinIterator(ChannelsMapperT.Channel) = .{},

    const Self = @This();
    pub const ParentConnT = Connection(.ClientConnectionType);
    const MessageHandlerT = MessageHandler(Self);
    const ChannelsMapperT = ChannelsMapper(ParentConnT);

    pub fn init(allocator: Allocator, app_name: []const u8) !*ClientHandler {
        var self = try allocator.create(ClientHandler);
        self.* = .{ .chan_mapper = try ChannelsMapperT.init(allocator), .tasks_queue = try TasksQueue.init(allocator), .allocator = allocator, .msg_store = ClientMessageStore{ .allocator = allocator }, .app_name = try allocator.dupe(u8, app_name) };
        return self;
    }

    pub fn deinit(self: *ClientHandler) void {
        self.allocator.free(self.app_name);
        self.allocator.destroy(self);
    }

    pub fn handler(self: *const Self) MessageHandlerT {
        return .{
            .ptr = @constCast(self),
            .handlers_mapping = MessageHandlerT.createMapping(.{ .HANDSHAKE = onHandshake, .REQUEST = onRequest, .EVENTS = onRequest, .RESPONSE = onResponse, .ERROR = onResponse }),
            .error_handler = errorHandler,
            .disconnection_handler = diconnectionHandler,
        };
    }

    pub fn findReceiverForMethod(self: *Self, method: []const u8, receiver: ?[]const u8) ![]const u8 {
        if (receiver) |rec| {
            if (mem.eql(u8, rec, self.app_name)) return error.ReceiverNotFound;
            var chan = self.chan_mapper.getChannel(rec) catch {
                print("Something went wrong. No channel for receiver: {s}\n", .{rec});
                return error.ReceiverNotFound;
            };
            if (chan.containsMethod(method)) return chan.connection_name;
        } else {
            var found_channels = std.ArrayList(ChannelsMapperT.Channel).init(self.allocator);
            self.mutex.lock();
            defer {
                self.mutex.unlock();
                found_channels.deinit();
                self.chan_iterator.reset();
            }
            for (self.chan_mapper.allChannels()) |*c| {
                if (mem.eql(u8, c.connection_name, self.app_name)) continue;
                if (c.containsMethod(method)) try found_channels.append(c.*);
            }
            if (self.chan_iterator.next(found_channels.items)) |*c| return c.connection_name;
        }
        return error.ReceiverNotFound;
    }

    pub fn setClientMethods(self: *Self, methods: []const u8) !void {
        if (self.methods) |m| self.allocator.free(m);
        self.methods = try self.allocator.dupe(u8, methods);
    }

    // ----------------------------- CLIENT HANDLERS -----------------------------
    pub fn onHandshake(self: *Self, conn: *ParentConnT, message: *Message) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        var client_hs = Handshake.fromJson(self.allocator, message.getData()) catch |err| {
            print("failed to parse handshake message: {s}", .{@errorName(err)});
            return;
        };
        defer client_hs.deinit();
        self.chan_mapper.clearAllChannels();
        for (client_hs.value.members) |mb| {
            var chan = try self.chan_mapper.getOrCreateChannel(conn, mb.name);
            if (mb.methods) |methods| {
                for (methods) |mt| try chan.addMethod(mt);
            }
        }
        if (std.mem.eql(u8, message.getTransmitter(), self.app_name)) {
            // Notify client worker about handshake completion.
            message.setDurable();
            try self.msg_store.insert(message);
        }
    }

    pub fn onRequest(self: *Self, _: *ParentConnT, message: *Message) !void {
        // put message to message queue for processing by client worker.
        message.setDurable();
        try self.tasks_queue.pushMessageToQueue(message);
    }

    pub fn onResponse(self: *Self, _: *ParentConnT, message: *Message) !void {
        // put message to client store for taking result by client.
        message.setDurable();
        try self.msg_store.insert(message);
    }

    // ---------------------- CLIENT CONNECTION LIFECYCLE ------------------------
    fn diconnectionHandler(_: *Self, _: *ParentConnT) !void {}

    fn errorHandler(_: *Self, conn: *ParentConnT, err: anyerror) !void {
        conn.suspended = true;
        // conn.kin.close(); // TODO: use deinit
        const addr = conn.getAddr() orelse null;
        switch (err) {
            error.EOF => print("connection closed by peer: {any}\n", .{addr}),
            error.IncompleteMessage => print("incomplete message from {any}\n", .{addr}),
            else => print("failed to receive message from none: {any}\n", .{err}),
        }
    }
};
