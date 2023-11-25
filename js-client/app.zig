const std = @import("std");
const Client = @import("../core/Client.zig");

const builtin = std.builtin;
const allocator = std.heap.page_allocator;

extern "env" fn _throwError(pointer: [*]const u8, length: u32) noreturn;
pub fn throwError(message: []const u8) noreturn {
    _throwError(message.ptr, message.len);
}

extern "env" fn _consoleLog(pointer: [*]const u8, length: u32) void;

pub fn consoleLog(comptime fmt: []const u8, args: anytype) void {
    const msg = std.fmt.allocPrint(allocator, fmt, args) catch
        @panic("failed to allocate memory for consoleLog message");
    defer allocator.free(msg);
    _consoleLog(msg.ptr, msg.len);
}

extern "window" fn send_to_socket(data_ptr: [*]const u8, data_len: u32) void;

// Calls to @panic are sent here.
// See https://ziglang.org/documentation/master/#panic
pub fn panic(message: []const u8, _: ?*builtin.StackTrace, _: ?usize) noreturn {
    throwError(message);
}

export fn allocUint8(length: u32) [*]const u8 {
    const slice = allocator.alloc(u8, length) catch
        @panic("failed to allocate memory");
    return slice.ptr;
}

export fn free(pointer: [*:0]const u8) void {
    allocator.free(std.mem.span(pointer));
}

export fn toSocket() void {
    const msg = "Hello from WebAssembly";
    send_to_socket(msg, msg.len);
}

export fn fromSocket(data: [*:0]const u8, len: u32) void {
    defer free(data);
    var msg = data[0..len];
    msg.len = len;
    consoleLog("{s} this is data\n", .{msg});
}

export fn init(data: [*:0]const u8) void {
    defer free(data);
    const app_name = std.mem.span(data);
    Client.init(allocator, app_name, .{}) catch |err| {
        throwError("failed to initialize client: {any}\n", .{err});
    };

    consoleLog("{s} this is data\n", .{app_name});
}
