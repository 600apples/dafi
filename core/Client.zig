const std = @import("std");
const os = std.os;
const net = std.net;
const tls = std.crypto.tls;
const Allocator = std.mem.Allocator;
const Bundle = std.crypto.Certificate.Bundle;

const Self = @This();

stream: net.Stream,
tls_client: ?tls.Client,
allocator: Allocator,

pub const Config = struct {
    max_size: usize = 65536,
    buffer_size: usize = 4096,
    tls: bool = false,
    ca_bundle: ?Bundle = null,
    handle_ping: bool = false,
    handle_pong: bool = false,
    handle_close: bool = false,
};

pub fn connect(allocator: Allocator, host: []const u8, port: u16, config: Config) !*Self {
    var tls_client: ?tls.Client = null;
    const net_stream = try net.tcpConnectToHost(allocator, host, port);

    if (config.tls) {
        var own_bundle = false;
        var bundle = config.ca_bundle orelse blk: {
            own_bundle = true;
            var b = Bundle{};
            try b.rescan(allocator);
            break :blk b;
        };
        tls_client = try tls.Client.init(net_stream, bundle, host);

        if (own_bundle) {
            bundle.deinit(allocator);
        }
    }
    var self = try allocator.create(Self);
    self.* = Self{
        .stream = net_stream,
        .tls_client = tls_client,
        .allocator = allocator,
    };
    return self;
}

pub fn close(self: *Self) void {
    self.stream.close();
    self.allocator.destroy(self);
}

pub fn read(self: *Self, buf: []u8) !usize {
    if (self.tls_client) |*tls_client| {
        return tls_client.read(self.stream, buf);
    }
    return self.stream.read(buf);
}

pub fn write(self: *Self, data: []const u8) !void {
    if (self.tls_client) |*tls_client| {
        return tls_client.writeAll(self.stream, data);
    }
    return self.stream.writeAll(data);
}

const zero_timeout = std.mem.toBytes(os.timeval{ .tv_sec = 0, .tv_usec = 0 });
pub fn writeTimeout(self: *const Self, ms: u32) !void {
    if (ms == 0) {
        return self.setsockopt(os.SO.SNDTIMEO, &zero_timeout);
    }

    const timeout = std.mem.toBytes(os.timeval{
        .tv_sec = @intCast(@divTrunc(ms, 1000)),
        .tv_usec = @intCast(@mod(ms, 1000) * 1000),
    });
    return self.setsockopt(os.SO.SNDTIMEO, &timeout);
}

pub fn receiveTimeout(self: *const Self, ms: u32) !void {
    if (ms == 0) {
        return self.setsockopt(os.SO.RCVTIMEO, &zero_timeout);
    }

    const timeout = std.mem.toBytes(os.timeval{
        .tv_sec = @intCast(@divTrunc(ms, 1000)),
        .tv_usec = @intCast(@mod(ms, 1000) * 1000),
    });
    return self.setsockopt(os.SO.RCVTIMEO, &timeout);
}

pub fn setsockopt(self: *const Self, optname: u32, value: []const u8) !void {
    return os.setsockopt(self.stream.handle, os.SOL.SOCKET, optname, value);
}
