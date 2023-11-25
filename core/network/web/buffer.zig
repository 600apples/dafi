const std = @import("std");

const Allocator = std.mem.Allocator;

pub const Buffer = struct {
    data: []u8,
    type: Type,

    const Type = enum {
        static,
        pooled,
        dynamic,
    };
};

// Provider manages all buffer access and types. It's where code goes to ask
// for and release buffers. One of the main reasons this exists is to handle
// the case where no Pool is configured, which is the default with client
// connections (unless a Pool is passed in the client config). Our reader
// doesn't really have to deal with that, it just calls provider.acquire()
// and it gets a buffer from somewhere.
pub const Provider = struct {
    pool: *Pool,
    allocator: Allocator,

    // If this is 0, pool is undefined. We need this field here anyways.
    pool_buffer_size: usize,

    pub fn initNoPool(allocator: Allocator) Provider {
        return init(allocator, undefined, 0);
    }

    pub fn init(allocator: Allocator, pool: *Pool, pool_buffer_size: usize) Provider {
        return .{
            .pool = pool,
            .allocator = allocator,
            .pool_buffer_size = pool_buffer_size,
        };
    }

    // should only be called when created via websocket.bufferPool, which exists
    // to make it easier for applications to manage a buffer pool across multiple
    // clients.
    pub fn deinit(self: *Provider) void {
        self.pool.deinit();
        self.allocator.destroy(self.pool);
        self.allocator.destroy(self);
    }

    pub fn static(self: Provider, size: usize) !Buffer {
        return .{
            .type = .static,
            .data = try self.allocator.alloc(u8, size),
        };
    }

    pub fn alloc(self: *Provider, size: usize) !Buffer {
        // remember: if self.pool_buffer_size == 0, then self.pool is undefined.
        if (size < self.pool_buffer_size) {
            if (self.pool.acquire()) |buffer| {
                return buffer;
            }
        }
        return .{
            .type = .dynamic,
            .data = try self.allocator.alloc(u8, size),
        };
    }

    pub fn free(self: *Provider, buffer: Buffer) void {
        switch (buffer.type) {
            .pooled => self.pool.release(buffer),
            .static => self.allocator.free(buffer.data),
            .dynamic => self.allocator.free(buffer.data),
        }
    }

    pub fn release(self: *Provider, buffer: Buffer) void {
        switch (buffer.type) {
            .static => {},
            .pooled => self.pool.release(buffer),
            .dynamic => self.allocator.free(buffer.data),
        }
    }
};

pub const Pool = struct {
    available: usize,
    buffers: []Buffer,
    allocator: Allocator,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: Allocator, count: usize, buffer_size: usize) !Pool {
        const buffers = try allocator.alloc(Buffer, count);

        for (0..count) |i| {
            buffers[i] = .{
                .type = .pooled,
                .data = try allocator.alloc(u8, buffer_size),
            };
        }

        return .{
            .mutex = .{},
            .buffers = buffers,
            .available = count,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Pool) void {
        const allocator = self.allocator;
        for (self.buffers) |buf| {
            allocator.free(buf.data);
        }
        allocator.free(self.buffers);
    }

    pub fn acquire(self: *Pool) ?Buffer {
        const buffers = self.buffers;

        self.mutex.lock();
        const available = self.available;
        if (available == 0) {
            // dont hold the lock over factory
            self.mutex.unlock();
            return null;
        }
        const index = available - 1;
        const buffer = buffers[index];
        self.available = index;
        self.mutex.unlock();

        return buffer;
    }

    pub fn release(self: *Pool, buffer: Buffer) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const available = self.available;
        self.buffers[available] = buffer;
        self.available = available + 1;
    }
};
