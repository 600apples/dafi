fn sum(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    _ = self;
    var a: c_long = undefined;
    var b: c_long = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "ll", &a, &b) != 0)) return Py_BuildValue("");
    return py.PyLong_FromLong((a + b));
}

fn mul(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    _ = self;
    var a: c_long = undefined;
    var b: c_long = undefined;
    if (PyArg_ParseTuple(args, "ll", &a, &b) == 0) return Py_BuildValue("");
    return PyLong_FromLong((a * b));
}

fn hello(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    _ = self;
    _ = args;
    print("welcom to ziglang\n", .{});
    return Py_BuildValue("");
}

fn printSt(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    _ = self;
    var input: [*:0]u8 = undefined;
    if (PyArg_ParseTuple(args, "s", &input) == 0) return Py_BuildValue("");
    print("you entered: {s}\n", .{input});
    return Py_BuildValue("");
}

fn returnArrayWithInput(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    _ = self;

    var N: u32 = undefined;
    if (!(py._PyArg_ParseTuple_SizeT(args, "l", &N) != 0)) return Py_BuildValue("");
    var list: [*c]PyObject = py.PyList_New(N);

    var i: u32 = 0;
    while (i < N) : (i += 1) {
        const python_int: [*c]PyObject = Py_BuildValue("i", i);
        _ = py.PyList_SetItem(list, i, python_int);
    }
    return list;
}
