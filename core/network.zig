pub const ClientConnection = @import("network/ClientConnection.zig");
pub const WasmConnection = @import("network/WasmConnection.zig");
pub const WebConnection = @import("network/WebConnection.zig");
pub const ServerConnection = @import("network/ServerConnection.zig");
pub const Connection = @import("network/connection.zig").Connection;
pub const ConnectionType = @import("network/connection.zig").ConnectionType;
pub const MessageHandlerType = @import("network/connection.zig").MessageHandlerType;
pub const OperationTable = @import("network/connection.zig").OperationTable;

test {
    _ = @import("network/connection.zig");
}
