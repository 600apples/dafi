const std = @import("std");
const io = std.io;
const os = std.os;
const net = std.net;
const ascii = std.ascii;
const serde = @import("../serde.zig");
const network = @import("../network.zig");
const handlers = @import("../handlers.zig");
const repository = @import("../repository.zig");
const ClientConnection = network.ClientConnection;
const WasmConnection = network.WasmConnection;
const WebConnection = network.WebConnection;
const SynParser = WebConnection.SynParser;
const ServerConnection = network.ServerConnection;
const Service = handlers.Service;
const Controller = handlers.Controller;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const is_wasm = @import("../misc.zig").is_wasm;

pub const ConnectionType = enum {
    ClientConnectionType,
    ServerConnectionType,
};

pub const OperationTable = struct {
    read: *const fn (ctx: *anyopaque, buf: []u8) anyerror!usize,
    readNoBuffer: *const fn (ctx: *anyopaque) anyerror!?[]const u8, // read to internal buffer
    write: *const fn (ctx: *anyopaque, data: []const u8) anyerror!void,
    accept: *const fn (ctx: *anyopaque) anyerror!net.StreamServer.Connection,
    close: *const fn (ctx: *anyopaque) void,
    getAddr: *const fn (ctx: *anyopaque) ?net.Address,
};

pub fn Connection(comptime contype: ConnectionType) type {
    const T = switch (contype) {
        .ClientConnectionType => if (is_wasm) WasmConnection else ClientConnection,
        .ServerConnectionType => ServerConnection,
    };

    return struct {
        ctx: *anyopaque,
        op_table: OperationTable,
        allocator: Allocator,
        wlock: bool = false,
        suspended: bool = false,
        is_websocket: bool = false,

        pub const Self = @This();
        pub const Config = T.Config;

        pub usingnamespace switch (contype) {
            .ClientConnectionType => blk: {
                if (is_wasm) break :blk struct {} else break :blk struct {
                    pub const ReadError = os.ReadError;
                    pub const WriteError = os.WriteError;

                    pub const Reader = io.Reader(Self, ReadError, read);
                    pub const Writer = io.Writer(Self, WriteError, write);

                    pub fn reader(self: *Self) Reader {
                        return .{ .context = self };
                    }

                    pub fn writer(self: *Self) Writer {
                        return .{ .context = self };
                    }

                    pub fn read(self: *Self, buf: []u8) !usize {
                        // if (self.kin.tls_client) |*tls_client| {
                        //     return tls_client.read(self.kin, buf);
                        // }
                        return self.op_table.read(self.ctx, buf);
                        // return self.kin.read(buf);
                    }

                    pub fn readNoBuffer(self: *Self) !?[]const u8 {
                        return self.op_table.readNoBuffer(self.ctx);
                    }

                    pub fn write(self: *Self, data: []const u8) !void {
                        while (@atomicRmw(bool, &self.wlock, .Xchg, true, .SeqCst)) {} // two threads should write at the same time
                        defer assert(@atomicRmw(bool, &self.wlock, .Xchg, false, .SeqCst));
                        // if (self.kin.tls_client) |*tls_client| {
                        //     return tls_client.writeAll(self.kin, data);
                        // }
                        try self.op_table.write(self.ctx, data);

                        // return self.kin.write(data);
                    }
                };
            },
            .ServerConnectionType => struct {
                const ClientConnectionType = Connection(.ClientConnectionType);

                pub fn accept(self: *Self) !*ClientConnectionType {
                    const conn = try self.op_table.accept(self.ctx);

                    var sync_parser = try SynParser.init(self.allocator);
                    defer sync_parser.deinit();
                    const addr = conn.address;
                    const is_websocket = try sync_parser.tryWebSocket(conn.stream);
                    if (is_websocket) {
                        std.debug.print("accepted web connection: {any}\n", .{addr});
                    } else {
                        std.debug.print("accepted connection: {any}\n", .{addr});
                    }
                    var new_self: *ClientConnectionType = try self.allocator.create(ClientConnectionType);

                    if (is_websocket) {
                        var client = try WebConnection.init(self.allocator, conn.stream, addr, .{});
                        new_self.* = .{ .ctx = client, .op_table = WebConnection.op_table, .allocator = self.allocator, .is_websocket = true };
                        return new_self;
                    } else {
                        var client = try self.allocator.create(ClientConnection);
                        client.* = .{ .stream = conn.stream, .addr = addr, .tls_client = null, .allocator = self.allocator };
                        new_self.* = .{ .ctx = client, .op_table = ClientConnection.op_table, .allocator = self.allocator };
                        return new_self;
                    }
                }
            },
        };

        pub fn init(allocator: Allocator, config: Config) !*Self {
            var self: *Self = try allocator.create(Self);
            var kin = try T.init(allocator, config);
            self.* = .{ .ctx = kin, .op_table = T.op_table, .allocator = allocator };
            return self;
        }

        pub fn close(self: *Self) void {
            self.op_table.close(self.ctx);
            self.allocator.destroy(self);
        }

        pub fn getAddr(self: *Self) ?net.Address {
            return self.op_table.getAddr(self.ctx);
        }
    };
}

test "test connection" {
    var allocator = std.testing.allocator;
    var conn = try Connection(.ClientConnectionType).init(allocator, "tcpbin.com", 4242, .{ .mode = .Controller }, false);
    conn.close();
}
