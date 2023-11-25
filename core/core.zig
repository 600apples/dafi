const py = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", {});
    @cInclude("Python.h");
});
const PyObject = py.PyObject;
const PyMethodDef = py.PyMethodDef;
const PyModuleDef = py.PyModuleDef;
const PyModuleDef_Base = py.PyModuleDef_Base;
const PyModule_Create = py.PyModule_Create;
const cffi = @import("cffi.zig");

var Methods = [_]PyMethodDef{
    PyMethodDef{
        .ml_name = "startServer",
        .ml_meth = cffi.startServer,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "stopServer",
        .ml_meth = cffi.stopServer,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "joinServer",
        .ml_meth = cffi.joinServer,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "detachServer",
        .ml_meth = cffi.detachServer,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "startClient",
        .ml_meth = cffi.startClient,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "stopClient",
        .ml_meth = cffi.stopClient,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "sendMessageFromClient",
        .ml_meth = cffi.sendMessageFromClient,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "sendHandshakeFromClient",
        .ml_meth = cffi.sendHandshakeFromClient,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "getAvailableMembers",
        .ml_meth = cffi.getAvailableMembers,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "sendMessageFromServer",
        .ml_meth = cffi.sendMessageFromServer,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "getMessageFromClientStore",
        .ml_meth = cffi.getMessageFromClientStore,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "setTimeoutError",
        .ml_meth = cffi.setTimeoutError,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "getMessageForClientWorker",
        .ml_meth = cffi.getMessageForClientWorker,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "getMessageForServerWorker",
        .ml_meth = cffi.getMessageForServerWorker,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "setServiceMethods",
        .ml_meth = cffi.setServiceMethods,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "testClient",
        .ml_meth = cffi.testClient,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "DoSome",
        .ml_meth = cffi.DoSome,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },
    PyMethodDef{
        .ml_name = "initThreads",
        .ml_meth = cffi.initThreads,
        .ml_flags = @as(c_int, 1),
        .ml_doc = null,
    },

    PyMethodDef{
        .ml_name = null,
        .ml_meth = null,
        .ml_flags = 0,
        .ml_doc = null,
    },
};

var module = PyModuleDef{
    .m_base = PyModuleDef_Base{
        .ob_base = PyObject{
            .ob_refcnt = 1,
            .ob_type = null,
        },
        .m_init = null,
        .m_index = 0,
        .m_copy = null,
    },
    .m_name = "dfcore",
    .m_doc = null,
    .m_size = -1,
    .m_methods = &Methods,
    .m_slots = null,
    .m_traverse = null,
    .m_clear = null,
    .m_free = null,
};

pub export fn PyInit_dfcore() [*]PyObject {
    return PyModule_Create(&module);
}
