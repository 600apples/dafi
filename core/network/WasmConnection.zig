const std = @import("std");
const Allocator = std.mem.Allocator;
const OperationTable = @import("../network.zig").OperationTable;

const WasmConnection = @This();

pub const op_table = OperationTable{
    .read = undefined, // wasm doesn't have net package functionality
    .readNoBuffer = undefined, // wasm doesn't have net package functionality
    .write = undefined, // wasm doesn't have net package functionality
    .accept = undefined, // wasm doesn't have net package functionality
    .close = close,
    .getAddr = undefined, // wasm doesn't have net package functionality
};

allocator: Allocator,

pub const Config = struct {};

pub fn init(allocator: Allocator, _: Config) !*WasmConnection {
    var self = try allocator.create(WasmConnection);
    self.* = WasmConnection{
        .allocator = allocator,
    };
    return self;
}

pub fn close(ctx: *anyopaque) void {
    const self: *WasmConnection = @ptrCast(@alignCast(ctx));
    self.allocator.destroy(self);
}
