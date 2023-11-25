const std = @import("std");
const os = std.os;
const net = std.net;
const tls = std.crypto.tls;
const Allocator = std.mem.Allocator;
const Bundle = std.crypto.Certificate.Bundle;
const OperationTable = @import("../network.zig").OperationTable;

const ClientConnection = @This();

pub const op_table = OperationTable{
    .read = read,
    .readNoBuffer = readNoBuffer,
    .write = write,
    .accept = accept,
    .close = close,
    .getAddr = getAddr,
};

stream: net.Stream,
tls_client: ?tls.Client,
addr: ?net.Address,
allocator: Allocator,

pub const Config = struct {
    host: []const u8,
    port: u16,
    tls: bool = false,
    ca_bundle: ?Bundle = null,
    password: []const u8 = "",
};
const zero_timeout = std.mem.toBytes(os.timeval{ .tv_sec = 0, .tv_usec = 0 });

pub fn init(allocator: Allocator, config: Config) !*ClientConnection {
    var tls_client: ?tls.Client = null;
    const stream = net.tcpConnectToHost(allocator, config.host, config.port) catch blk: {
        break :blk try net.connectUnixSocket(config.host);
    };

    if (config.tls) {
        var own_bundle = false;
        var bundle = config.ca_bundle orelse blk: {
            own_bundle = true;
            var b = Bundle{};
            try b.rescan(allocator);
            break :blk b;
        };
        tls_client = try tls.Client.init(stream, bundle, config.host);

        if (own_bundle) {
            bundle.deinit(allocator);
        }
    }
    var self = try allocator.create(ClientConnection);
    self.* = ClientConnection{
        .stream = stream,
        .addr = null,
        .tls_client = tls_client,
        .allocator = allocator,
    };

    try self.writeTimeout(1000);
    return self;
}

pub fn read(ctx: *anyopaque, buf: []u8) !usize {
    const self: *ClientConnection = @ptrCast(@alignCast(ctx));
    return try self.stream.readAll(buf);
}

pub fn readNoBuffer(_: *anyopaque) !?[]const u8 {
    @panic("readNoBuffer not supported for client connections");
}

pub fn write(ctx: *anyopaque, data: []const u8) !void {
    const self: *ClientConnection = @ptrCast(@alignCast(ctx));
    try self.stream.writeAll(data);
}

pub fn accept(_: *anyopaque) !net.StreamServer.Connection {
    @panic("accept not supported for client connections");
}

pub fn close(ctx: *anyopaque) void {
    const self: *ClientConnection = @ptrCast(@alignCast(ctx));
    self.stream.close();
    self.allocator.destroy(self);
}

pub fn getAddr(ctx: *anyopaque) ?net.Address {
    const self: *ClientConnection = @ptrCast(@alignCast(ctx));
    if (self.addr) |addr| return addr;
    return null;
}

pub fn writeTimeout(self: *ClientConnection, ms: u32) !void {
    if (ms == 0) {
        return self.setsockopt(os.SO.SNDTIMEO, &zero_timeout);
    }

    const timeout = std.mem.toBytes(os.timeval{
        .tv_sec = @intCast(@divTrunc(ms, 1000)),
        .tv_usec = @intCast(@mod(ms, 1000) * 1000),
    });
    return self.setsockopt(os.SO.SNDTIMEO, &timeout);
}

pub fn receiveTimeout(self: *ClientConnection, ms: u32) !void {
    if (ms == 0) {
        return self.setsockopt(os.SO.RCVTIMEO, &zero_timeout);
    }
    const timeout = std.mem.toBytes(os.timeval{
        .tv_sec = @intCast(@divTrunc(ms, 1000)),
        .tv_usec = @intCast(@mod(ms, 1000) * 1000),
    });
    return self.setscokopt(os.SO.RCVTIMEO, &timeout);
}

fn setsockopt(self: *ClientConnection, optname: u32, value: []const u8) !void {
    return os.setsockopt(self.stream.handle, os.SOL.SOCKET, optname, value);
}
