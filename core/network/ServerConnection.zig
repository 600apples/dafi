const std = @import("std");
const net = std.net;
const StreamServer = net.StreamServer;
const Allocator = std.mem.Allocator;
const Bundle = std.crypto.Certificate.Bundle;
const HandlerMode = @import("../handlers.zig").HandlerMode;
const OperationTable = @import("../network.zig").OperationTable;

const ServerConnection = @This();

pub const op_table = OperationTable{
    .read = read,
    .readNoBuffer = readNoBuffer,
    .write = write,
    .accept = accept,
    .close = close,
    .getAddr = getAddr,
};

listener: net.StreamServer,
addr: net.Address,
allocator: Allocator,

pub const Config = struct {
    host: ?[]const u8,
    port: u16,
    tls: bool = false,
    ca_bundle: ?Bundle = null,
    mode: HandlerMode,
    password: []const u8,
};

pub fn init(allocator: Allocator, config: Config) !*ServerConnection {
    var maybe_host = config.host orelse "0.0.0.0";
    var listener = StreamServer.init(.{});
    const addr = net.Address.resolveIp(maybe_host, config.port) catch try net.Address.initUnix(maybe_host);
    try listener.listen(addr);

    var self = try allocator.create(ServerConnection);
    self.* = ServerConnection{
        .listener = listener,
        .addr = addr,
        .allocator = allocator,
    };
    return self;
}

pub fn read(_: *anyopaque, _: []u8) !usize {
    @panic("server connection does not support read");
}

pub fn readNoBuffer(_: *anyopaque) !?[]const u8 {
    @panic("server connection does not support readNoBuffer");
}

pub fn write(_: *anyopaque, _: []const u8) !void {
    @panic("server connection does not support write");
}

pub fn accept(ctx: *anyopaque) !StreamServer.Connection {
    const self: *ServerConnection = @ptrCast(@alignCast(ctx));
    return try self.listener.accept();
}

pub fn close(ctx: *anyopaque) void {
    const self: *ServerConnection = @ptrCast(@alignCast(ctx));
    if (self.listener.sockfd != null) {
        // Prevent double close (might be by signal and by user action)
        self.listener.deinit();
        self.allocator.destroy(self);
    }
}

pub fn getAddr(ctx: *anyopaque) ?net.Address {
    const self: *ServerConnection = @ptrCast(@alignCast(ctx));
    return self.addr;
}
