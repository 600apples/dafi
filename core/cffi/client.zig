const py = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", {});
    @cInclude("Python.h");
});
const PyObject = py.PyObject;
const Py_BuildValue = py.Py_BuildValue;
const PyArg_ParseTuple = py.PyArg_ParseTuple;
const PyErr_SetString = py.PyErr_SetString;

const std = @import("std");
const serde = @import("../serde.zig");
const handlers = @import("../handlers.zig");
const MessageFlag = serde.MessageFlag;
const MessageDecoder = serde.MessageDecoder;
const Client = @import("../Client.zig");
const HandlerMode = handlers.HandlerMode;

// var tha = std.heap.ThreadSafeAllocator{ .child_allocator = std.heap.c_allocator };
const allocator = std.heap.c_allocator;

pub fn initThreads(_: [*c]PyObject, _: [*c]PyObject) callconv(.C) [*]PyObject {
    py.PyEval_InitThreads();
    return Py_BuildValue("");
}

pub fn testClient(_: [*c]PyObject, _: [*c]PyObject) callconv(.C) [*]PyObject {
    // var dsc: [*:0]u8 = undefined;
    // py.PyObject_CopyData(&dsc, src);
    // if (!(py._PyArg_ParseTuple_SizeT(src, "y*", &dsc) != 0)) return Py_BuildValue("");
    return Py_BuildValue("");

    // py.PyEval_InitThreads();
    // var _save = py.PyEval_SaveThread();
    //
    // //
    // while (true) {
    //     std.time.sleep(1000);
    // }
    // py.PyEval_RestoreThread(_save);

}

pub fn DoSome(_: [*c]PyObject, _: [*c]PyObject) callconv(.C) [*]PyObject {
    std.debug.print("DoSome\n", .{});
    var _save = py.PyEval_SaveThread();

    while (true) {
        std.time.sleep(1000);
    }
    py.PyEval_RestoreThread(_save);

    return Py_BuildValue("");
}

pub fn startClient(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var host: [*:0]u8 = undefined;
    var port: c_long = undefined;
    var password: [*:0]u8 = undefined;
    var app_name: [*:0]u8 = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "slss", &host, &port, &password, &app_name) != 0)) return Py_BuildValue("");
    var pport: u16 = @intCast(port);
    var phost = std.mem.span(host);
    var ppassword = std.mem.span(password);
    var papp_name = std.mem.span(app_name);
    const conn_num = Client.init(allocator, papp_name, .{ .host = phost, .port = pport, .password = ppassword }) catch return Py_BuildValue("");
    var result = Py_BuildValue("k", @as(c_ulong, conn_num));
    return result;
}

pub fn stopClient(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "k", &conn_num) != 0)) return Py_BuildValue("");
    Client.desctroyClient(conn_num) catch return Py_BuildValue("");
    return Py_BuildValue("");
}

pub fn sendMessageFromClient(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var data: *PyObject = undefined;
    var uuid: c_uint = undefined;
    var flag: std.meta.Tag(MessageFlag) = undefined;
    var decoder: std.meta.Tag(MessageDecoder) = undefined;
    var is_bytes: u1 = undefined;
    var receiver: [*:0]u8 = undefined;
    var func_name: [*:0]u8 = undefined;
    var return_result: u1 = undefined;
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "OIHHpsspk", &data, &uuid, &flag, &decoder, &is_bytes, &receiver, &func_name, &return_result, &conn_num) != 0)) {
        PyErr_SetString(py.PyExc_ValueError, "unable to parse provided arguments");
        return Py_BuildValue("");
    }
    var src = py.PyBytes_FromObject(data);
    var size: i64 = 0;
    var buffer: [*]u8 = undefined;
    if (py.PyBytes_AsStringAndSize(src, @ptrCast(&buffer), &size) < 0) {
        return Py_BuildValue("");
    }
    const pdata = buffer[0..@as(usize, @intCast(size))];
    const puuid: u16 = @as(u16, @truncate(uuid));
    const pflag: MessageFlag = @enumFromInt(flag);
    const pdecoder: MessageDecoder = @enumFromInt(decoder);
    const pis_bytes = if (is_bytes == 0) false else true;
    const preceiver = std.mem.span(receiver);
    const pfunc_name = std.mem.span(func_name);
    const preturn_result = if (return_result == 0) false else true;
    const msgident = Client.sendMessage(pdata, puuid, pflag, pdecoder, pis_bytes, preturn_result, preceiver, pfunc_name, conn_num) catch |err| {
        PyErr_SetString(py.PyExc_ValueError, @errorName(err));
        return Py_BuildValue("");
    };
    const found_receiver: []const u8 = msgident.receiver;
    return Py_BuildValue("(Iks#)", msgident.uuid, @as(c_long, msgident.timestamp), found_receiver.ptr, found_receiver.len);
}

pub fn sendHandshakeFromClient(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var password: [*:0]u8 = undefined;
    var methods: [*:0]u8 = undefined;
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "ssk", &password, &methods, &conn_num) != 0)) {
        PyErr_SetString(py.PyExc_ValueError, "unable to parse provided arguments");
        return Py_BuildValue("");
    }
    const ppassword = std.mem.span(password);
    const pmethods = std.mem.span(methods);
    const msgident = Client.sendHandshake(conn_num, ppassword, pmethods) catch |err| {
        PyErr_SetString(py.PyExc_ValueError, @errorName(err));
        return Py_BuildValue("");
    };
    const found_receiver: []const u8 = msgident.receiver;
    return Py_BuildValue("(Iks#)", msgident.uuid, @as(c_long, msgident.timestamp), found_receiver.ptr, found_receiver.len);
}

pub fn getMessageFromClientStore(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var uuid: c_uint = undefined;
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "Ik", &uuid) != 0)) return Py_BuildValue("");
    const msg = (Client.getMessageByUuid(@as(u16, @truncate(uuid)), conn_num) catch |err| {
        var err_name = @errorName(err)[0..];
        return Py_BuildValue("(s#)", err_name.ptr, err_name.len);
    }) orelse return Py_BuildValue("");
    defer msg.undurableAndDeinit();
    const data: []const u8 = msg.getData();
    const flag: c_ushort = @intFromEnum(msg.getFlag());
    const decoder: c_ushort = @intFromEnum(msg.getDecoder());
    const template = if (msg.isBytes()) "(y#IH)" else "(s#IH)";
    return Py_BuildValue(template, data.ptr, data.len, flag, decoder);
}

pub fn setTimeoutError(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var uuid: c_uint = undefined;
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "Ik", &uuid, &conn_num) != 0)) return Py_BuildValue("");
    Client.setTimeoutError(@as(u16, @truncate(uuid)), conn_num) catch return Py_BuildValue("");
    return Py_BuildValue("");
}

pub fn getMessageForClientWorker(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "k", &conn_num) != 0)) return Py_BuildValue("");
    var msg = (Client.getMessageForClientWorker(conn_num) catch return Py_BuildValue("")) orelse return Py_BuildValue("");
    defer msg.undurableAndDeinit();
    const uuid: c_uint = @as(c_uint, msg.getUuid());
    const data: []const u8 = msg.getData();
    const flag: c_ushort = @intFromEnum(msg.getFlag());
    const decoder: c_ushort = @intFromEnum(msg.getDecoder());
    const transmitter: []const u8 = msg.getTransmitter();
    const receiver: []const u8 = msg.getReceiver();
    const func_name: []const u8 = msg.getFuncName();
    const return_result: c_ushort = @intFromBool(msg.getReturnResult());
    const template = if (msg.isBytes()) "(Iy#IHs#s#s#H)" else "(Is#IHs#s#s#H)";
    return Py_BuildValue(template, uuid, data.ptr, data.len, flag, decoder, transmitter.ptr, transmitter.len, receiver.ptr, receiver.len, func_name.ptr, func_name.len, return_result);
}

pub fn getAvailableMembers(_: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    var conn_num: usize = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "k", &conn_num) != 0)) return Py_BuildValue("");
    var members = Client.getAvailableMembers(allocator, conn_num) catch return Py_BuildValue("");
    return Py_BuildValue("s#", members.ptr, members.len);
}
