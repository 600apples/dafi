const std = @import("std");
const Client = @import("./Client.zig");
const builtin = std.builtin;
const ArenaAllocator = std.heap.ArenaAllocator;
const allocator = std.heap.wasm_allocator;
const MessageDecoder = @import("serde.zig").MessageDecoder;

pub const os = struct {
    pub const system = struct {
        pub const fd_t = u8;
        pub const STDERR_FILENO = 1;
        pub const E = std.os.linux.E;

        pub const sockaddr = extern union {
            pub const in = u8;
            pub const in6 = u8;
        };

        pub fn getErrno(T: usize) E {
            _ = T;
            return .SUCCESS;
        }

        pub fn write(f: fd_t, ptr: [*]const u8, len: usize) usize {
            _ = ptr;
            _ = f;
            return len;
        }
    };
};

extern "env" fn _throwError(pointer: [*]const u8, length: u32) noreturn;
extern "env" fn _consoleLog(pointer: [*]const u8, length: u32) void;
extern "window" fn _sendToSocket(data_ptr: [*]const u8, data_len: u32) void;
extern "window" fn _storeMessage(data_ptr: [*]const u8, data_len: u32, uuid: u32, is_error: bool) void;
extern "window" fn _triggerEvent(data_ptr: [*]const u8, data_len: u32) void;

pub fn throwError(message: []const u8) noreturn {
    _throwError(message.ptr, message.len);
}

pub fn consoleLog(comptime fmt: []const u8, args: anytype) void {
    const msg = std.fmt.allocPrint(allocator, fmt, args) catch
        @panic("failed to allocate memory for consoleLog message");
    defer allocator.free(msg);
    _consoleLog(msg.ptr, msg.len);
}

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

export fn sendHandshake(password: [*:0]const u8, conn_num: usize) u32 {
    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();
    var arena_allocator = arena.allocator();
    const actual_password: []const u8 = std.mem.span(password);
    defer allocator.free(actual_password);
    const methods = ""; // Empty for now. Maybe in the future we will have some methods to be executed in browser.
    const ident = Client.createHandshake(arena_allocator, actual_password, methods, conn_num) catch throwError("failed to create message\n");
    _sendToSocket(ident.data.ptr, ident.data.len);
    return ident.message.uuid;
}

export fn sendMessage(data: [*:0]const u8, receiver: [*:0]const u8, func_name: [*:0]const u8, serde: u32, return_result: bool, conn_num: usize) [*]const usize {
    const actual_data: []const u8 = std.mem.span(data);
    const actual_receiver: []const u8 = std.mem.span(receiver);
    const actual_func_name: []const u8 = std.mem.span(func_name);
    const decoder: MessageDecoder = @enumFromInt(serde);
    const is_bytes = switch (decoder) {
        .JSON => false,
        .RAW => true,
        else => unreachable,
    };
    defer {
        allocator.free(actual_data);
        allocator.free(actual_receiver);
        allocator.free(actual_func_name);
    }
    const ident = Client.createMessage(allocator, actual_data, 0, .REQUEST, decoder, is_bytes, return_result, actual_receiver, actual_func_name, conn_num) catch |err| {
        const available_receivers = Client.getAvailableMembers(allocator, conn_num) catch throwError("failed to get available receivers\n");
        var iterator = std.mem.splitSequence(u8, available_receivers, "members");
        _ = iterator.next();
        const members = iterator.next() orelse throwError("failed to get members\n");
        // consoleLog("failed to create message: {any} for method '{s}'.\nAvailable receivers {s}\n", .{ err, actual_func_name, available_receivers });
        var buf: [1000]u8 = undefined;
        _ = std.fmt.bufPrint(&buf, "failed to create message: {any} for method '{s}'.\nAvailable receivers {?s}\n", .{ err, actual_func_name, std.mem.trim(u8, members, ":}") }) catch
            @panic("failed to allocate memory for error message");
        throwError(&buf);
    };
    const result = allocator.alloc(usize, 4) catch @panic("failed to allocate memory for result");
    _sendToSocket(ident.data.ptr, ident.data.len);
    result[0] = ident.message.uuid;
    result[1] = ident.message.receiver.len;
    result[2] = @intFromPtr(ident.message.receiver.ptr);
    result[3] = @intFromBool(!is_bytes);
    return result.ptr;
}

export fn parseAndStoreMessage(data: [*:0]const u8, len: u32, conn_num: u32) void {
    defer free(data);
    var actual_data = data[0..len];
    actual_data.len = len;
    var entry = Client.getClientEntry(conn_num) catch throwError("failed to get client entry\n");
    var client_handler = entry.client_handler;
    var connection = entry.connection;
    var msg = entry.msgpool.buildWsMessage(actual_data) catch throwError("failed to build framed message\n");
    defer msg.deinit();
    const uuid = msg.getUuid();
    var payload = msg.getData();
    switch (msg.getFlag()) {
        .HANDSHAKE => {
            client_handler.onHandshake(connection, msg) catch |err| {
                consoleLog("failed to parse and store message: {any}\n", .{err});
                throwError("failed to parse and store message\n");
            };
            if (std.mem.eql(u8, msg.getTransmitter(), client_handler.app_name)) {
                // Notify sender about handshake completion.
                _storeMessage(payload.ptr, payload.len, uuid, false);
            }
        },
        .RESPONSE => _storeMessage(payload.ptr, payload.len, uuid, false),
        .ERROR => _storeMessage(payload.ptr, payload.len, uuid, true),
        .EVENTS => _triggerEvent(payload.ptr, payload.len),
        else => unreachable,
    }
}

export fn initClient(data: [*:0]const u8) usize {
    defer free(data);
    const app_name = std.mem.span(data);
    return Client.init(allocator, app_name, .{}) catch throwError("failed to initialize client\n");
}
