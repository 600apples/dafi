# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import messager_pb2 as messager__pb2


class MessagerServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.communicate = channel.stream_stream(
            "/MessagerService/communicate",
            request_serializer=messager__pb2.Message.SerializeToString,
            response_deserializer=messager__pb2.Message.FromString,
        )
        self.stream_to_controller = channel.stream_unary(
            "/MessagerService/stream_to_controller",
            request_serializer=messager__pb2.Message.SerializeToString,
            response_deserializer=messager__pb2.Empty.FromString,
        )
        self.stream_from_controller = channel.unary_stream(
            "/MessagerService/stream_from_controller",
            request_serializer=messager__pb2.Empty.SerializeToString,
            response_deserializer=messager__pb2.Message.FromString,
        )


class MessagerServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def communicate(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def stream_to_controller(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def stream_from_controller(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_MessagerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "communicate": grpc.stream_stream_rpc_method_handler(
            servicer.communicate,
            request_deserializer=messager__pb2.Message.FromString,
            response_serializer=messager__pb2.Message.SerializeToString,
        ),
        "stream_to_controller": grpc.stream_unary_rpc_method_handler(
            servicer.stream_to_controller,
            request_deserializer=messager__pb2.Message.FromString,
            response_serializer=messager__pb2.Empty.SerializeToString,
        ),
        "stream_from_controller": grpc.unary_stream_rpc_method_handler(
            servicer.stream_from_controller,
            request_deserializer=messager__pb2.Empty.FromString,
            response_serializer=messager__pb2.Message.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("MessagerService", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class MessagerService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def communicate(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/MessagerService/communicate",
            messager__pb2.Message.SerializeToString,
            messager__pb2.Message.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def stream_to_controller(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_unary(
            request_iterator,
            target,
            "/MessagerService/stream_to_controller",
            messager__pb2.Message.SerializeToString,
            messager__pb2.Empty.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def stream_from_controller(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/MessagerService/stream_from_controller",
            messager__pb2.Empty.SerializeToString,
            messager__pb2.Message.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
