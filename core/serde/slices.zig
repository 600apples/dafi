const std = @import("std");
const Allocator = std.mem.Allocator;
const expect = std.testing.expect;

/// Allocate a new slice that is the concatenation of the given slices. Caller owns returned slice and must free memory.
pub fn concatenateSlicesAlloc(allocator: Allocator, slices: []const []const u8) ![]u8 {
    var total_len: usize = 0;
    for (slices) |slice| total_len += slice.len;

    var current_pos: usize = 0;
    var result: []u8 = try allocator.alloc(u8, total_len);
    for (slices) |slice| {
        @memcpy(result[current_pos .. current_pos + slice.len], slice);
        current_pos += slice.len;
    }
    return result;
}

/// Allocate a new null terminated slice that is the concatenation of the given slices. Caller owns returned slice and must free memory.
pub fn concatenateSlicesAllocZ(allocator: Allocator, slices: []const []const u8) ![:0]u8 {
    var total_len: usize = 1;
    for (slices) |slice| total_len += slice.len;

    var current_pos: usize = 0;
    var result = try allocator.alloc(u8, total_len);
    for (slices) |slice| {
        @memcpy(result[current_pos .. current_pos + slice.len], slice);
        current_pos += slice.len;
    }
    result[total_len - 1] = 0;
    return result[0 .. total_len - 1 :0];
}

pub fn concatenateSlicesBuf(buf: []u8, slices: []const []const u8) void {
    var total_len: usize = 0;
    for (slices) |slice| total_len += slice.len;

    var current_pos: usize = 0;
    for (slices) |slice| {
        @memcpy(buf[current_pos .. current_pos + slice.len], slice);
        current_pos += slice.len;
    }
}

test "concatenate slices alloc" {
    var sl1 = "foo";
    var sl2 = "bar";
    var sl3 = "baz";
    var sl4 = "qux";

    var alloc = std.testing.allocator;
    var result = try concatenateSlicesAlloc(alloc, &[_][]const u8{ sl1, sl2, sl3, sl4 });
    defer alloc.free(result);
    try expect(std.mem.eql(u8, result, "foobarbazqux"));
}

test "concatenate slices alloc Z" {
    var sl1 = "foo";
    var sl2 = "bar";
    var sl3 = "baz";
    var sl4 = "qux";

    var alloc = std.testing.allocator;
    var result = try concatenateSlicesAllocZ(alloc, &[_][]const u8{ sl1, sl2, sl3, sl4 });
    defer alloc.free(result);
    try expect(std.mem.eql(u8, result, "foobarbazqux"));
    try expect(@TypeOf(result) == [:0]u8);
    try expect(result.len == 12);
    try expect(result[12] == 0);
}

test "concatenate slices buf" {
    var sl1 = "foo";
    var sl2 = "bar";
    var sl3 = "baz";
    var sl4 = "qux";

    var buf: [12]u8 = undefined;
    concatenateSlicesBuf(&buf, &[_][]const u8{ sl1, sl2, sl3, sl4 });
    try expect(std.mem.eql(u8, &buf, "foobarbazqux"));
}
