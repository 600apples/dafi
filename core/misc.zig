const std = @import("std");
const builtin = @import("builtin");
pub const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;
pub const print = if (is_wasm) @import("wasm.zig").consoleLog else std.debug.print;
