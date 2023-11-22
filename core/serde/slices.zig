const std = @import("std");
const Allocator = std.mem.Allocator;

/// Allocate a new slice that is the concatenation of the given slices. Caller owns returned slice and must free memory.
pub fn concatenateSlicesAlloc(allocator: Allocator, slices: []const []const u8) ![]const u8 {
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
    try std.testing.expect(std.mem.eql(u8, result, "foobarbazqux"));
}

test "concatenate slices buf" {
    var sl1 = "foo";
    var sl2 = "bar";
    var sl3 = "baz";
    var sl4 = "qux";

    var buf: [12]u8 = undefined;
    concatenateSlicesBuf(&buf, &[_][]const u8{ sl1, sl2, sl3, sl4 });
    try std.testing.expect(std.mem.eql(u8, &buf, "foobarbazqux"));
}
