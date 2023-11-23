const std = @import("std");
const Client = @import("./Client.zig");
const MessagePool = @import("./serde.zig").MessagePool;

test {
    var alloc = std.testing.allocator;
    var msgpool = try MessagePool.init(alloc);

    const config = Client.Config{};
    var client = try Client.connect(alloc, "127.0.0.1", 5000, config);
    defer client.close();
    try msgpool.send(client, "hello world@", "transmitter@", "receiver@", "func_name@", 555, true, "metadata", .EMPTY);
}
