import logging
from collections import defaultdict

from anyio import Event, create_task_group
from anyio.abc._sockets import SocketStream

from dafi.message import Message, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace
from dafi.utils.settings import (
    CONTROLLER_CALLBACK_MAPPING,
    AWAITED_PROCS,
    WELL_KNOWN_CALLBACKS,
    search_remote_callback_in_mapping,
)
from dafi.components.operations.socket_store import SocketStore


class ControllerOperations:
    """Controller operations specification object"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.socket_store = SocketStore()

    @with_debug_trace
    async def on_stream_close(self, stream: SocketStream):

        del_proc = await self.socket_store.find_process_name_by_stream(stream)
        await self.socket_store.delete_stream(del_proc)
        CONTROLLER_CALLBACK_MAPPING.pop(del_proc, None)

        # Delete message pointers that deleted process expects to obtain.
        awaited_by_deleted_proc = [uuid for uuid, (trans, rec) in AWAITED_PROCS.items() if trans == del_proc]
        for uuid in awaited_by_deleted_proc:
            AWAITED_PROCS.pop(uuid, None)

        # Find receivers that awaiting result from deleted process
        awaited_transmitters = defaultdict(list)
        [awaited_transmitters[trans].append(uuid) for uuid, (trans, rec) in AWAITED_PROCS.items() if rec == del_proc]

        async for proc, sock in self.socket_store.iterate():
            # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
            await sock.send(
                Message(
                    flag=MessageFlag.UPDATE_CALLBACKS,
                    transmitter=del_proc,
                    receiver=proc,
                    func_args=(CONTROLLER_CALLBACK_MAPPING,),
                )
            )
            awaited_messages = awaited_transmitters.get(proc)
            if awaited_messages:
                for msg_uuid in awaited_messages:
                    await sock.send(
                        Message(
                            flag=MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY,
                            transmitter=del_proc,
                            receiver=proc,
                            uuid=msg_uuid,
                            error=RemoteError(info=f"Remote process {del_proc} stopped unexpectedly."),
                        )
                    )

    @with_debug_trace
    async def on_handshake(self, msg: Message, stream: SocketStream, global_terminate_event: Event):
        self.socket_store.add_stream(
            stream=stream, process_name=msg.transmitter, global_terminate_event=global_terminate_event
        )
        CONTROLLER_CALLBACK_MAPPING[msg.transmitter] = msg.func_args[0]
        msg.func_args = (CONTROLLER_CALLBACK_MAPPING,)

        async with create_task_group() as sg:
            async for proc, sock in self.socket_store.iterate():
                # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
                msg.receiver = proc
                sg.start_soon(sock.send, msg)

    @with_debug_trace
    async def on_update_callbacks(self, msg: Message):
        CONTROLLER_CALLBACK_MAPPING[msg.transmitter].update(msg.func_args[0])
        msg.func_args = (CONTROLLER_CALLBACK_MAPPING,)

        async with create_task_group() as sg:
            async for proc, sock in self.socket_store.iterate():
                # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
                msg.receiver = proc
                sg.start_soon(sock.send, msg)

    @with_debug_trace
    async def on_request(self, msg: Message, global_terminate_event: Event):
        receiver = msg.receiver
        trans_sock = self.socket_store.get(msg.transmitter)

        if not receiver:
            data = search_remote_callback_in_mapping(
                CONTROLLER_CALLBACK_MAPPING, msg.func_name, exclude=msg.transmitter
            )
            if data:
                receiver, _ = data

        if receiver and receiver in CONTROLLER_CALLBACK_MAPPING:
            msg.receiver = receiver
            # Take socket of destination process where function/method will be triggered.
            sock = self.socket_store.get(receiver)
            if sock:
                # Assign transmitter to message id in order to redirect result after processing.
                AWAITED_PROCS[msg.uuid] = msg.transmitter, receiver

                await sock.send(msg)
                if msg.period:
                    # Return back to transmitter info that scheduled task was accepted.
                    msg.swap_applicants()
                    msg.flag = MessageFlag.SCHEDULER_ACCEPT
                    await trans_sock.send(msg)

        else:
            msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE
            info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
            msg.error = RemoteError(info=info)
            await trans_sock.send(msg)
            self.logger.error(info)

    @with_debug_trace
    async def on_success(self, msg: Message):
        transmitter, _ = AWAITED_PROCS.pop(msg.uuid, (None, None))
        if not transmitter:
            sock = self.socket_store[msg.transmitter]
            msg.flag = MessageFlag.UNABLE_TO_FIND_PROCESS
            info = "Unable to find process by message uuid"
            msg.error = RemoteError(info=info)
            await sock.send(msg)
            self.logger.error(info)

        else:
            sock = self.socket_store.get(transmitter)
            if not sock:
                logging.error(f"Unable to send calculated result. Process {transmitter!r} is disconnected.")
            else:
                await sock.send(msg)

    @with_debug_trace
    async def on_scheduler_error(self, msg: Message):
        # Dont care about message uuid. Scheduler tasks are long running processes. Sheduler can report about error
        # multiple times.
        AWAITED_PROCS.pop(msg.uuid, None)
        sock = self.socket_store.get(msg.receiver)
        if not sock:
            logging.error(f"Unable to send calculated result. Process {msg.receiver!r} is disconnected.")
        else:
            msg.flag = MessageFlag.SUCCESS
            await sock.send(msg)

    @with_debug_trace
    async def on_broadcast(self, msg: Message, process_name: str):

        if msg.func_name in WELL_KNOWN_CALLBACKS:
            async with create_task_group() as sg:
                async for receiver, sock in self.socket_store.iterate():
                    if receiver == process_name:
                        # Do not send broadcast message to itself!
                        continue
                    msg.receiver = receiver
                    sg.start_soon(sock.send, msg)
        else:
            data = search_remote_callback_in_mapping(
                CONTROLLER_CALLBACK_MAPPING, msg.func_name, exclude=msg.transmitter, take_all=True
            )
            if data:
                async with create_task_group() as sg:
                    for receiver, _ in data:
                        if receiver == process_name:
                            # Do not send broadcast message to itself!
                            continue
                        msg.receiver = receiver
                        # Take socket of destination process where function/method will be triggered.
                        sock = self.socket_store.get(receiver)
                        if sock:
                            sg.start_soon(sock.send, msg)

    async def close_sockets(self):
        await self.socket_store.close()
        CONTROLLER_CALLBACK_MAPPING.clear()
        AWAITED_PROCS.clear()
