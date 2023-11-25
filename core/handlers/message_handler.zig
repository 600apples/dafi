const std = @import("std");
const enums = std.enums;
const meta = std.meta;
const net = std.net;
const serde = @import("../serde.zig");
const Message = serde.Message;
const MessageFlag = serde.MessageFlag;

pub fn MessageHandler(comptime T: type) type {
    return struct {
        ptr: *T,
        handlers_mapping: HandlersMapping,
        error_handler: *const fn (ctx: *T, conn: *T.ParentConnT, err: anyerror) anyerror!void,
        disconnection_handler: *const fn (ctx: *T, conn: *T.ParentConnT) anyerror!void,

        const Self = @This();

        pub const HandlersMapping = [meta.fields(MessageFlag).len]HandlerEntryFn;
        const HandlerEntryFn = *const fn (ctx: *T, conn: *T.ParentConnT, message: *Message) anyerror!void;

        pub fn createMapping(smapping: enums.EnumFieldStruct(MessageFlag, HandlerEntryFn, notSupported)) HandlersMapping {
            const fields = meta.fields(MessageFlag);
            var mapping: [fields.len]HandlerEntryFn = undefined;
            inline for (fields) |field| {
                mapping[field.value] = @field(smapping, field.name);
            }
            return mapping;
        }

        pub fn handle(self: Self, conn: *T.ParentConnT, message: *Message) !void {
            switch (message.getFlag()) {
                inline else => |fl| try self.handlers_mapping[@intFromEnum(fl)](self.ptr, conn, message),
            }
        }

        pub fn handleErr(self: Self, conn: *T.ParentConnT, err: anyerror) !void {
            try self.error_handler(self.ptr, conn, err);
        }

        pub fn handleDisconnect(self: Self, conn: *T.ParentConnT) !void {
            try self.disconnection_handler(self.ptr, conn);
        }

        fn notSupported(_: *T, _: *T.ParentConnT, message: *Message) !void {
            std.debug.print("message flag {any} not supported\n", .{message.getFlag()});
            return error.FlagNotSupported;
        }
    };
}
