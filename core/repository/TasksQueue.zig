const std = @import("std");
const network = @import("../network.zig");
const handlers = @import("../handlers.zig");
const serde = @import("../serde.zig");
const Message = serde.Message;
const Allocator = std.mem.Allocator;
const LinkedList = std.DoublyLinkedList(*Message);

const TasksQueue = @This();

allocator: Allocator,
queue: LinkedList,

pub fn init(allocator: std.mem.Allocator) !TasksQueue {
    var queue = LinkedList{};
    return .{ .allocator = allocator, .queue = queue };
}

pub fn deinit(self: *TasksQueue) void {
    while (self.queue.popFirst()) |*node| self.allocator.destroy(node);
}

pub fn getMessageFromQueue(self: *TasksQueue) ?*Message {
    if (self.queue.popFirst()) |node| {
        defer self.allocator.destroy(node);
        return node.data;
    }
    return null;
}

pub fn pushMessageToQueue(self: *TasksQueue, message: *Message) !void {
    const node = try self.allocator.create(LinkedList.Node);
    node.* = .{ .data = message };
    self.queue.append(node);
}

pub fn shiftMessageToQueue(self: *TasksQueue, message: *Message) !void {
    const node = try self.allocator.create(LinkedList.Node);
    node.* = .{ .data = message };
    self.queue.prepend(node);
}

pub fn len(self: *TasksQueue) usize {
    return self.queue.len;
}
