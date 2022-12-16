import logging
import os
import time
from functools import partial
from collections import defaultdict
from typing import NoReturn, ClassVar, Dict

from anyio import (
    create_task_group,
    sleep,
    move_on_after,
    create_unix_listener,
    TASK_STATUS_IGNORED,
    EndOfStream,
    BrokenResourceError,
)
from anyio.abc import TaskGroup
from anyio.abc._sockets import SocketStream

from dafi.utils import colors
from dafi.backend import BackEndKeys
from dafi.utils.logger import patch_logger
from dafi.components import UnixComponentBase
from dafi.message import Message, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace
from dafi.utils.mappings import CONTROLLER_CALLBACK_MAPPING, AWAITED_PROCS, search_cb_info_in_mapping


logger = patch_logger(logging.getLogger(__name__), colors.blue)


class Controller(UnixComponentBase):
    @with_debug_trace
    async def handle(self, *, task_status=TASK_STATUS_IGNORED):
        self.operations = ControllerOperations()

        if os.path.exists(self.socket):
            os.remove(self.socket)

        async with create_task_group() as sg:
            self.listener = await create_unix_listener(self.socket)
            task_status.started("STARTED")
            sg.start_soon(self.healthcheck)
            sg.start_soon(self.listener.serve, partial(self._handle_commands, sg=sg), sg)
            logger.info(f"Controller has been started successfully. Process name: {self.process_name!r}")

    @with_debug_trace
    async def healthcheck(self) -> NoReturn:
        while not self.stop_event.is_set():
            self.backend.write(BackEndKeys.CONTROLLER_HEALTHCHECK_TS, time.time())
            await sleep(self.TIMEOUT / 2)

    @with_debug_trace
    async def _handle_commands(self, stream: SocketStream, sg: TaskGroup):

        async with stream:
            while not self.stop_event.is_set():
                with move_on_after(1) as scope:
                    try:
                        raw_msglen = await stream.receive(4)
                    except (EndOfStream, BrokenResourceError, ConnectionResetError):
                        await self.operations.on_stream_close(stream)
                        break

                if scope.cancel_called or not raw_msglen:
                    continue

                msglen = Message.msglen(raw_msglen)
                msg = Message.loads(await stream.receive(msglen))

                if msg.flag == MessageFlag.HANDSHAKE:
                    if not await self.operations.on_handshake(msg, stream, sg):
                        break

                elif msg.flag == MessageFlag.UPDATE_CALLBACKS:
                    await self.operations.on_update_callbacks(msg, sg)

                elif msg.flag == MessageFlag.REQUEST:
                    await self.operations.on_request(msg, stream, sg)

                elif msg.flag == MessageFlag.SUCCESS:
                    await self.operations.on_success(msg, sg)

        if self.stop_event.is_set():
            await self.listener.aclose()
            await sg.cancel_scope.cancel()


class ControllerOperations:
    """Controller operations specification object"""

    socket_store: ClassVar[Dict[str, SocketStream]] = dict()

    @with_debug_trace
    async def on_stream_close(self, stream: SocketStream):
        del_proc = None
        try:
            del_proc = next(proc for proc, sock in self.socket_store.items() if sock == stream)
            del self.socket_store[del_proc]
            CONTROLLER_CALLBACK_MAPPING.pop(del_proc, None)
        except StopIteration:
            ...

        # Delete message pointers that deleted process expects to obtain.
        awaited_by_deleted_proc = [uuid for uuid, (trans, rec) in AWAITED_PROCS.items() if trans == del_proc]
        for uuid in awaited_by_deleted_proc:
            AWAITED_PROCS.pop(uuid, None)

        # Find receivers that awaiting result from deleted process
        awaited_transmitters = defaultdict(list)
        [awaited_transmitters[trans].append(uuid) for uuid, (trans, rec) in AWAITED_PROCS.items() if rec == del_proc]

        for proc, sock in self.socket_store.items():
            # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
            await sock.send(
                Message(
                    flag=MessageFlag.UPDATE_CALLBACKS,
                    transmitter=del_proc,
                    receiver=proc,
                    func_args=(CONTROLLER_CALLBACK_MAPPING,),
                ).dumps()
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
                        ).dumps()
                    )

    @with_debug_trace
    async def on_handshake(self, msg: Message, stream: SocketStream, sg: TaskGroup):
        if msg.transmitter in self.socket_store:
            info = f"Detected 2 remote processes with the name {msg.transmitter!r}. Please rename one of them."
            logger.error(info)
            msg.error = RemoteError(info=info)
            await stream.send(msg.dumps())
            return False
        else:
            self.socket_store[msg.transmitter] = stream
            CONTROLLER_CALLBACK_MAPPING[msg.transmitter] = msg.func_args[0]
            msg.func_args = (CONTROLLER_CALLBACK_MAPPING,)
            for proc, sock in self.socket_store.items():
                # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
                msg.receiver = proc
                sg.start_soon(sock.send, msg.dumps())
            return True

    @with_debug_trace
    async def on_update_callbacks(self, msg: Message, sg: TaskGroup):
        CONTROLLER_CALLBACK_MAPPING[msg.transmitter].update(msg.func_args[0])
        msg.func_args = (CONTROLLER_CALLBACK_MAPPING,)
        for proc, sock in self.socket_store.items():
            # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
            msg.receiver = proc
            sg.start_soon(sock.send, msg.dumps())

    @with_debug_trace
    async def on_request(self, msg: Message, stream: SocketStream, sg: TaskGroup):
        receiver = msg.receiver
        if not receiver:
            data = search_cb_info_in_mapping(CONTROLLER_CALLBACK_MAPPING, msg.func_name, exclude=msg.transmitter)
            if data:
                receiver, _ = data

        if receiver and receiver in CONTROLLER_CALLBACK_MAPPING:
            msg.receiver = receiver
            # Take socket of destination process where function/method will be triggered.
            sock = self.socket_store[receiver]
            # Assign trasmitter to message id in order to redirect result after processing.
            AWAITED_PROCS[msg.uuid] = msg.transmitter, receiver
            sg.start_soon(sock.send, msg.dumps())

        else:
            msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE
            info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
            msg.error = RemoteError(info=info)
            sg.start_soon(stream.send, msg.dumps())
            logger.error(info)

    @with_debug_trace
    async def on_success(self, msg: Message, sg: TaskGroup):
        transmitter, _ = AWAITED_PROCS.pop(msg.uuid, (None, None))
        if not transmitter:
            sock = self.socket_store[msg.transmitter]
            msg.flag = MessageFlag.UNABLE_TO_FIND_PROCESS
            info = "Unable to find process by message uuid"
            msg.error = RemoteError(info=info)
            sg.start_soon(sock.send, msg.dumps())
            logger.error(info)

        else:
            sock = self.socket_store.get(transmitter)
            if not sock:
                logging.error(f"Unable to send calculated result. Process {transmitter!r} is disconnected.")
            else:
                sg.start_soon(sock.send, msg.dumps())
