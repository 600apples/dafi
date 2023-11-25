const std = @import("std");
const mem = std.mem;
const net = std.net;
const ascii = std.ascii;
const Mutex = std.Thread.Mutex;
const Allocator = std.mem.Allocator;

pub const KeyValue = struct {
    len: usize,
    keys: [][]const u8,
    values: [][]const u8,

    const Self = @This();

    pub fn init(allocator: Allocator, max: usize) !Self {
        const keys = try allocator.alloc([]const u8, max);
        const values = try allocator.alloc([]const u8, max);
        return Self{
            .len = 0,
            .keys = keys,
            .values = values,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.keys);
        allocator.free(self.values);
    }

    pub fn add(self: *Self, key: []const u8, value: []const u8) void {
        const len = self.len;
        var keys = self.keys;
        if (len == keys.len) {
            return;
        }

        keys[len] = key;
        self.values[len] = value;
        self.len = len + 1;
    }

    pub fn get(self: Self, needle: []const u8) ?[]const u8 {
        const keys = self.keys[0..self.len];
        for (keys, 0..) |key, i| {
            if (mem.eql(u8, key, needle)) {
                return self.values[i];
            }
        }
        return null;
    }

    pub fn reset(self: *Self) void {
        self.len = 0;
    }
};

pub const Handshake = struct {
    url: []const u8,
    key: []const u8,
    method: []const u8,
    headers: *KeyValue,
    raw_header: []const u8,

    pub fn parse(buf: []u8, headers: *KeyValue) !Handshake {
        var data = buf;
        const request_line_end = mem.indexOfScalar(u8, data, '\r') orelse unreachable;
        var request_line = data[0..request_line_end];

        if (!ascii.endsWithIgnoreCase(request_line, "http/1.1")) {
            return error.InvalidProtocol;
        }

        var key: []const u8 = "";
        var required_headers: u8 = 0;

        var request_length = request_line_end;

        data = data[request_line_end + 2 ..];

        while (data.len > 4) {
            const index = mem.indexOfScalar(u8, data, '\r') orelse unreachable;
            const separator = mem.indexOfScalar(u8, data[0..index], ':') orelse return error.InvalidHeader;

            const name = mem.trim(u8, toLower(data[0..separator]), &ascii.whitespace);
            const value = mem.trim(u8, data[(separator + 1)..index], &ascii.whitespace);
            headers.add(name, value);

            if (mem.eql(u8, "upgrade", name)) {
                if (!ascii.eqlIgnoreCase("websocket", value)) {
                    return error.InvalidUpgrade;
                }
                required_headers |= 1;
            } else if (mem.eql(u8, "sec-websocket-version", name)) {
                if (!mem.eql(u8, "13", value)) {
                    return error.InvalidVersion;
                }
                required_headers |= 2;
            } else if (mem.eql(u8, "connection", name)) {
                // find if connection header has upgrade in it, example header:
                //		Connection: keep-alive, Upgrade
                if (ascii.indexOfIgnoreCase(value, "upgrade") == null) {
                    return error.InvalidConnection;
                }
                required_headers |= 4;
            } else if (mem.eql(u8, "sec-websocket-key", name)) {
                key = value;
                required_headers |= 8;
            }
            const next = index + 2;
            request_length += next;
            data = data[next..];
        }

        if (required_headers != 15) {
            return error.MissingHeaders;
        }

        // we already established that request_line ends with http/1.1, so this buys
        // us some leeway into parsing it
        const separator = mem.indexOfScalar(u8, request_line, ' ') orelse return error.InvalidRequestLine;
        const method = request_line[0..separator];
        const url = mem.trim(u8, request_line[separator + 1 .. request_line.len - 9], &ascii.whitespace);

        return .{
            .key = key,
            .url = url,
            .method = method,
            .headers = headers,
            .raw_header = buf[request_line_end + 2 .. request_length + 2],
        };
    }

    pub fn close(stream: anytype, err: anyerror) !void {
        try stream.writeAll("HTTP/1.1 400 Invalid\r\nerror: ");
        const s = switch (err) {
            error.Empty => "empty",
            error.InvalidProtocol => "invalidprotocol",
            error.InvalidRequestLine => "invalidrequestline",
            error.InvalidHeader => "invalidheader",
            error.InvalidUpgrade => "invalidupgrade",
            error.InvalidVersion => "invalidversion",
            error.InvalidConnection => "invalidconnection",
            error.MissingHeaders => "missingheaders",
            else => "unknown",
        };
        try stream.writeAll(s);
        try stream.writeAll("\r\n\r\n");
    }

    pub fn reply(self: Handshake, stream: anytype) !void {
        var h: [20]u8 = undefined;

        // HTTP/1.1 101 Switching Protocols\r\n
        // Upgrade: websocket\r\n
        // Connection: upgrade\r\n
        // Sec-Websocket-Accept: BASE64_ENCODED_KEY_HASH_PLACEHOLDER_000\r\n\r\n
        var buf = [_]u8{ 'H', 'T', 'T', 'P', '/', '1', '.', '1', ' ', '1', '0', '1', ' ', 'S', 'w', 'i', 't', 'c', 'h', 'i', 'n', 'g', ' ', 'P', 'r', 'o', 't', 'o', 'c', 'o', 'l', 's', '\r', '\n', 'U', 'p', 'g', 'r', 'a', 'd', 'e', ':', ' ', 'w', 'e', 'b', 's', 'o', 'c', 'k', 'e', 't', '\r', '\n', 'C', 'o', 'n', 'n', 'e', 'c', 't', 'i', 'o', 'n', ':', ' ', 'u', 'p', 'g', 'r', 'a', 'd', 'e', '\r', '\n', 'S', 'e', 'c', '-', 'W', 'e', 'b', 's', 'o', 'c', 'k', 'e', 't', '-', 'A', 'c', 'c', 'e', 'p', 't', ':', ' ', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '\r', '\n', '\r', '\n' };
        const key_pos = buf.len - 32;

        var hasher = std.crypto.hash.Sha1.init(.{});
        hasher.update(self.key);
        hasher.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        hasher.final(&h);

        _ = std.base64.standard.Encoder.encode(buf[key_pos .. key_pos + 28], h[0..]);
        try stream.writeAll(&buf);
    }
};

fn toLower(str: []u8) []u8 {
    for (str, 0..) |c, i| {
        str[i] = ascii.toLower(c);
    }
    return str;
}

// This is what we're pooling
const HandshakeState = struct {
    // a buffer to read data into
    buffer: []u8,

    // Headers
    headers: KeyValue,

    fn init(allocator: Allocator, buffer_size: usize, max_headers: usize) !*HandshakeState {
        const hs = try allocator.create(HandshakeState);
        hs.* = .{
            .buffer = try allocator.alloc(u8, buffer_size),
            .headers = try KeyValue.init(allocator, max_headers),
        };
        return hs;
    }

    fn deinit(self: *HandshakeState, allocator: Allocator) void {
        allocator.free(self.buffer);
        self.headers.deinit(allocator);
        allocator.destroy(self);
    }

    fn reset(self: *HandshakeState) void {
        self.headers.reset();
    }
};

pub const Pool = struct {
    mutex: Mutex,
    available: usize,
    allocator: Allocator,
    buffer_size: usize,
    max_headers: usize,
    states: []*HandshakeState,

    pub fn init(allocator: Allocator, count: usize, buffer_size: usize, max_headers: usize) !*Pool {
        var self = try allocator.create(Pool);
        errdefer allocator.destroy(self);
        const states = try allocator.alloc(*HandshakeState, count);
        errdefer allocator.free(states);
        for (0..count) |i| {
            states[i] = try HandshakeState.init(allocator, buffer_size, max_headers);
        }
        self.* = .{
            .mutex = Mutex{},
            .states = states,
            .allocator = allocator,
            .available = count,
            .max_headers = max_headers,
            .buffer_size = buffer_size,
        };
        return self;
    }

    pub fn deinit(self: *Pool) void {
        const allocator = self.allocator;
        for (self.states) |s| {
            s.deinit(allocator);
        }
        allocator.free(self.states);
    }

    pub fn acquire(self: *Pool) !*HandshakeState {
        const states = self.states;
        self.mutex.lock();
        const available = self.available;
        if (available == 0) {
            // dont hold the lock over factory
            self.mutex.unlock();
            return try HandshakeState.init(self.allocator, self.buffer_size, self.max_headers);
        }
        const index = available - 1;
        const state = states[index];
        self.available = index;
        self.mutex.unlock();
        return state;
    }

    pub fn release(self: *Pool, state: *HandshakeState) void {
        state.reset();
        var states = self.states;

        self.mutex.lock();
        const available = self.available;
        if (available == states.len) {
            self.mutex.unlock();
            state.deinit(self.allocator);
            return;
        }
        states[available] = state;
        self.available = available + 1;
        self.mutex.unlock();
    }
};
