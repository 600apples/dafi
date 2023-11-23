const std = @import("std");
const net = std.net;
const StreamServer = net.StreamServer;

const MessagePool = @import("./serde.zig").MessagePool;

const Server = @This();
const log = std.log.scoped(.server);

pub fn listen(config: ServerConfig, msgpool: *MessagePool) !void {
    var listener = StreamServer.init(.{
        .reuse_address = true,
        .kernel_backlog = 1024,
    });
    defer listener.deinit();

    const address = blk: {
        if (config.unix_path) |unix_path| {
            std.fs.deleteFileAbsolute(unix_path) catch {};
            break :blk try net.Address.initUnix(unix_path);
        } else {
            break :blk try net.Address.parseIp(config.address, config.port);
        }
    };
    _ = try listener.listen(address);

    while (true) {
        if (listener.accept()) |connection| {
            const args = .{ connection, &config, msgpool };
            const thread = try std.Thread.spawn(.{}, clientLoop, args);
            thread.detach();
        } else |err| {
            log.err("failed to accept connection {}", .{err});
        }
    }
}

fn clientLoop(conn: StreamServer.Connection, config: *const ServerConfig, msgpool: *MessagePool) !void {
    _ = config;
    const conn_address = conn.address;
    const stream = conn.stream;

    log.debug("accepted connection from {any}\n", .{conn_address});

    while (true) {
        if (msgpool.receive(stream)) |maybe_message| {
            if (maybe_message) |*message| {
                defer message.deinit();
                std.debug.print("received message: {any}\n", .{message});
            } else {
                std.debug.print("empty message\n", .{});

            }
        } else |err| {
            log.err("failed to receive message: {}", .{err});
            break;
        }
    }
}

pub const ServerConfig = struct {
    address: []const u8,
    port: u16,
    unix_path: ?[]const u8,
};

test "listen" {
    var alloc = std.testing.allocator;
    var msgpool = try MessagePool.init(alloc);

    var config = ServerConfig{
        .port = 5000,
        .address = "127.0.0.1",
        .unix_path = null,
    };
    try Server.listen(config, &msgpool);
}
