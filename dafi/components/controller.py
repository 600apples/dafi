import logging
import time
from functools import partial
from collections import defaultdict
from typing import NoReturn, ClassVar, Dict

from anyio import (
    Event,
    create_task_group,
    sleep,
    move_on_after,
    TASK_STATUS_IGNORED,
)
from anyio.abc import TaskGroup
from anyio.abc._sockets import SocketStream

from dafi.utils import colors
from dafi.backend import BackEndKeys
from dafi.utils.logger import patch_logger
from dafi.components import ComponentsBase, send_to_stream
from dafi.message import Message, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace
from dafi.utils.retry import stoppable_retry, RetryInfo
from dafi.utils.mappings import (
    CONTROLLER_CALLBACK_MAPPING,
    AWAITED_PROCS,
    search_remote_callback_in_mapping,
)


logger = patch_logger(logging.getLogger(__name__), colors.blue)


class Controller(ComponentsBase):
    @stoppable_retry(wait=3)
    async def handle(
        self,
        global_event,
        retry_info: RetryInfo,
        task_status=TASK_STATUS_IGNORED,
    ):
        self.operations = ControllerOperations()

        if global_event.is_set():
            logger.info("Termination controller...")
            return

        if retry_info.attempt == 2 or not retry_info.attempt % 5:
            logger.error(f"Unable to connect controller. Error = {retry_info.prev_error}. Retrying...")

        async with create_task_group() as sg:
            self.listener = await self.create_listener()
            if task_status._future._state == "PENDING":
                task_status.started("STARTED")
            sg.start_soon(self.healthcheck)
            sg.start_soon(self.listener.serve, partial(self._handle_commands, sg=sg), sg)
            logger.info(f"Controller has been started successfully. Process name: {self.process_name!r}")

    @with_debug_trace
    async def healthcheck(self) -> NoReturn:
        while True:
            self.backend.write(BackEndKeys.CONTROLLER_HEALTHCHECK_TS, time.time())
            await sleep(self.TIMEOUT / 2)

    @with_debug_trace
    async def _handle_commands(self, stream: SocketStream, sg: TaskGroup):

        stop_event = Event()
        async with stream:
            while not stop_event.is_set():
                with move_on_after(1) as scope:
                    try:
                        raw_msglen = await stream.receive(4)
                        if not raw_msglen:
                            continue
                        msg = await Message.loads(stream, raw_msglen)
                        if not msg:
                            continue
                    except Exception:
                        await self.operations.on_stream_close(stream)
                        break

                if scope.cancel_called:
                    continue

                if msg.flag == MessageFlag.HANDSHAKE:
                    await self.operations.on_handshake(msg, stream, sg, stop_event)

                elif msg.flag == MessageFlag.UPDATE_CALLBACKS:
                    await self.operations.on_update_callbacks(msg, sg)

                elif msg.flag == MessageFlag.REQUEST:
                    await self.operations.on_request(msg, stream, sg, stop_event)

                elif msg.flag == MessageFlag.SUCCESS:
                    await self.operations.on_success(msg, sg)

                elif msg.flag == MessageFlag.SCHEDULER_ERROR:
                    await self.operations.on_scheduler_error(msg, sg)

                elif msg.flag == MessageFlag.BROADCAST:
                    await self.operations.on_broadcast(msg, sg)


class ControllerOperations:
    """Controller operations specification object"""

    socket_store: ClassVar[Dict[str, SocketStream]] = dict()

    @with_debug_trace
    async def on_stream_close(self, stream: SocketStream):
        del_proc = None
        try:
            del_proc = next(proc for proc, (sock, event) in list(self.socket_store.items()) if sock == stream)
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

        for proc, (sock, event) in list(self.socket_store.items()):
            # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
            await send_to_stream(
                sock,
                Message(
                    flag=MessageFlag.UPDATE_CALLBACKS,
                    transmitter=del_proc,
                    receiver=proc,
                    func_args=(CONTROLLER_CALLBACK_MAPPING,),
                ).dumps(),
                event,
            )
            awaited_messages = awaited_transmitters.get(proc)
            if awaited_messages:
                for msg_uuid in awaited_messages:
                    await send_to_stream(
                        sock,
                        Message(
                            flag=MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY,
                            transmitter=del_proc,
                            receiver=proc,
                            uuid=msg_uuid,
                            error=RemoteError(info=f"Remote process {del_proc} stopped unexpectedly."),
                        ).dumps(),
                        event,
                    )

    @with_debug_trace
    async def on_handshake(self, msg: Message, stream: SocketStream, sg: TaskGroup, stop_event: Event):
        self.socket_store[msg.transmitter] = (stream, stop_event)
        CONTROLLER_CALLBACK_MAPPING[msg.transmitter] = msg.func_args[0]
        msg.func_args = (CONTROLLER_CALLBACK_MAPPING,)
        for proc, (sock, event) in list(self.socket_store.items()):
            # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
            msg.receiver = proc
            sg.start_soon(send_to_stream, sock, msg.dumps(), event)

    @with_debug_trace
    async def on_update_callbacks(self, msg: Message, sg: TaskGroup):
        CONTROLLER_CALLBACK_MAPPING[msg.transmitter].update(msg.func_args[0])
        msg.func_args = (CONTROLLER_CALLBACK_MAPPING,)
        for proc, (sock, event) in list(self.socket_store.items()):
            # Send updated CONTROLLER_CALLBACK_MAPPING to all processes including transmitter process.
            msg.receiver = proc
            sg.start_soon(send_to_stream, sock, msg.dumps(), event)

    @with_debug_trace
    async def on_request(self, msg: Message, stream: SocketStream, sg: TaskGroup, stop_event: Event):
        receiver = msg.receiver

        if not receiver:
            data = search_remote_callback_in_mapping(
                CONTROLLER_CALLBACK_MAPPING, msg.func_name, exclude=msg.transmitter
            )
            if data:
                receiver, _ = data

        if receiver and receiver in CONTROLLER_CALLBACK_MAPPING:
            msg.receiver = receiver
            # Take socket of destination process where function/method will be triggered.
            sock_and_event = self.socket_store.get(receiver)
            if sock_and_event:
                sock, event = sock_and_event
                # Assign transmitter to message id in order to redirect result after processing.
                AWAITED_PROCS[msg.uuid] = msg.transmitter, receiver
                sg.start_soon(send_to_stream, sock, msg.dumps(), event)

                if msg.period:
                    # Return back to transmitter info that scheduled task was accepted.
                    msg.swap_applicants()
                    msg.flag = MessageFlag.SCHEDULER_ACCEPT
                    sg.start_soon(send_to_stream, stream, msg.dumps(), stop_event)

        else:
            msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE
            info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
            msg.error = RemoteError(info=info)
            sg.start_soon(send_to_stream, stream, msg.dumps(), stop_event)
            logger.error(info)

    @with_debug_trace
    async def on_success(self, msg: Message, sg: TaskGroup):
        transmitter, _ = AWAITED_PROCS.pop(msg.uuid, (None, None))
        if not transmitter:
            sock, event = self.socket_store[msg.transmitter]
            msg.flag = MessageFlag.UNABLE_TO_FIND_PROCESS
            info = "Unable to find process by message uuid"
            msg.error = RemoteError(info=info)
            sg.start_soon(send_to_stream, sock, msg.dumps(), event)
            logger.error(info)

        else:
            sock_and_event = self.socket_store.get(transmitter)
            if not sock_and_event:
                logging.error(f"Unable to send calculated result. Process {transmitter!r} is disconnected.")
            else:
                sock, event = sock_and_event
                sg.start_soon(send_to_stream, sock, msg.dumps(), event)

    @with_debug_trace
    async def on_scheduler_error(self, msg: Message, sg: TaskGroup):
        # Dont care about message uuid. Scheduler tasks are long running processes. Sheduler can report about error
        # multiple times.
        AWAITED_PROCS.pop(msg.uuid, None)
        sock_and_event = self.socket_store.get(msg.receiver)
        if not sock_and_event:
            logging.error(f"Unable to send calculated result. Process {msg.receiver!r} is disconnected.")
        else:
            sock, event = sock_and_event
            msg.flag = MessageFlag.SUCCESS
            sg.start_soon(send_to_stream, sock, msg.dumps(), event)

    @with_debug_trace
    async def on_broadcast(self, msg: Message, sg: TaskGroup):
        data = search_remote_callback_in_mapping(
            CONTROLLER_CALLBACK_MAPPING, msg.func_name, exclude=msg.transmitter, take_all=True
        )
        if data:
            for receiver, _ in data:

                msg.receiver = receiver
                # Take socket of destination process where function/method will be triggered.
                sock_and_event = self.socket_store.get(receiver)
                if sock_and_event:
                    sock, event = sock_and_event
                    sg.start_soon(send_to_stream, sock, msg.dumps(), event)
