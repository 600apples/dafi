import logging
from collections import defaultdict
from typing import Dict, Tuple, Any

from anyio import sleep
from anyio.abc import TaskGroup
from daffi.utils.debug import with_debug_trace
from daffi.utils.settings import WELL_KNOWN_CALLBACKS
from daffi.utils.custom_types import K, GlobalCallback
from daffi.utils.misc import search_remote_callback_in_mapping
from daffi.components.operations.channel_store import ChannelStore, ChannelPipe, FreezableQueue
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag, RemoteError


BROADCAST_RESULT_EMPTY = object()


class ControllerOperations:
    """Controller operations specification object"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.channel_store = ChannelStore()
        self.controller_callback_mapping: Dict[K, Dict[K, GlobalCallback]] = dict()
        self.awaited_procs: Dict[str, Tuple[str, str]] = dict()
        self.awaited_broadcast_procs: Dict[str, Tuple[str, Dict[str, Any]]] = dict()

    async def wait_all_requests_done(self):
        while self.awaited_procs or self.awaited_broadcast_procs:
            await sleep(0.1)

    @with_debug_trace
    async def on_channel_close(self, channel: ChannelPipe, process_identificator: str):
        if channel.locked:
            return

        del_proc = await self.channel_store.find_process_name_by_channel(channel)
        await self.channel_store.delete_channel(del_proc)
        self.controller_callback_mapping.pop(del_proc, None)
        FreezableQueue.factory_remove(process_identificator)

        # Delete RpcMessage pointers that deleted process expects to obtain.
        awaited_by_deleted_proc = [uuid for uuid, (trans, rec) in self.awaited_procs.items() if trans == del_proc]
        for uuid in awaited_by_deleted_proc:
            self.awaited_procs.pop(uuid, None)
        awaited_by_deleted_proc = [
            uuid for uuid, (trans, rec) in self.awaited_broadcast_procs.items() if trans == del_proc
        ]
        for uuid in awaited_by_deleted_proc:
            self.awaited_broadcast_procs.pop(uuid, None)

        # Find receivers that awaiting result from deleted process (regular messages)
        awaited_transmitters = defaultdict(list)
        [
            awaited_transmitters[trans].append(uuid)
            for uuid, (trans, rec) in self.awaited_procs.items()
            if rec == del_proc
        ]

        async for proc, chan in self.channel_store.iterate():
            # Send updated self.controller_callback_mapping to all processes including transmitter process.
            chan.send(
                ServiceMessage(
                    flag=MessageFlag.UPDATE_CALLBACKS,
                    transmitter=del_proc,
                    receiver=proc,
                    data=self.controller_callback_mapping,
                )
            )
            awaited_messages = awaited_transmitters.get(proc)
            if awaited_messages:
                for msg_uuid in awaited_messages:
                    chan.send(
                        RpcMessage(
                            flag=MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY,
                            transmitter=del_proc,
                            receiver=proc,
                            uuid=msg_uuid,
                            error=RemoteError(info=f"Remote process {del_proc} stopped unexpectedly."),
                        )
                    )

        # Find receivers that awaiting result from deleted process (broadcast messages)
        awaited_transmitters = defaultdict(list)
        [
            awaited_transmitters[trans].append(uuid)
            for uuid, (trans, agg_msg) in self.awaited_broadcast_procs.items()
            if del_proc in agg_msg
        ]

        async for proc, chan in self.channel_store.iterate():
            awaited_messages = awaited_transmitters.get(proc)
            if awaited_messages:
                for msg_uuid in awaited_messages:
                    _, aggregated_result = self.awaited_broadcast_procs[msg_uuid]
                    aggregated_result[del_proc] = RemoteError(info=f"Remote process {del_proc} stopped unexpectedly.")
                    if all(res != BROADCAST_RESULT_EMPTY for res in aggregated_result.values()):
                        chan.send(
                            RpcMessage(
                                flag=MessageFlag.SUCCESS,
                                receiver=proc,
                                uuid=msg_uuid,
                                func_args=(aggregated_result,),
                            )
                        )

    @with_debug_trace
    async def on_handshake(self, msg: ServiceMessage, channel: ChannelPipe):
        msg.loads()
        self.controller_callback_mapping[msg.transmitter] = msg.data
        msg.set_data(self.controller_callback_mapping)

        was_locked = await self.channel_store.add_channel(channel=channel, process_name=msg.transmitter)
        if not was_locked:
            async for proc, chan in self.channel_store.iterate():
                # Send updated self.controller_callback_mapping to all processes except transmitter process.
                chan.send(msg.copy(transmitter=msg.transmitter, receiver=proc))

    @with_debug_trace
    async def on_request(self, msg: RpcMessage):

        receiver = msg.receiver
        transmitter = msg.transmitter
        trans_chan = await self.channel_store.get_chan(transmitter)

        if not receiver:
            data = search_remote_callback_in_mapping(
                self.controller_callback_mapping, msg.func_name, exclude=transmitter
            )
            if data:
                receiver, _ = data

        if receiver and receiver in self.controller_callback_mapping:
            msg.receiver = receiver
            # Take channel of destination process where function/method will be triggered.
            chan = await self.channel_store.get_chan(receiver)

            if chan:
                # Assign transmitter to RpcMessage id in order to redirect result after processing.
                self.awaited_procs[msg.uuid] = transmitter, receiver
                chan.send(msg)

                if msg.period and trans_chan:
                    # Return back to transmitter info that scheduled task was accepted.
                    trans_chan.send(
                        msg.copy(flag=MessageFlag.SCHEDULER_ACCEPT, transmitter=receiver, receiver=transmitter)
                    )

        else:
            msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE
            info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
            msg.set_error(RemoteError(info=info))
            if trans_chan:
                trans_chan.send(msg)
            self.logger.error(info)

    @with_debug_trace
    async def on_success(self, msg: RpcMessage):
        transmitter = None

        if msg.uuid in self.awaited_procs:
            transmitter, _ = self.awaited_procs.pop(msg.uuid)

        elif msg.uuid in self.awaited_broadcast_procs:
            msg.loads()
            transmitter, aggregated_result = self.awaited_broadcast_procs.get(msg.uuid)
            aggregated_result[msg.transmitter] = msg.error or msg.func_args[0]
            if any(res == BROADCAST_RESULT_EMPTY for res in aggregated_result.values()):
                # Not all results aggregated yet
                return
            else:
                del self.awaited_broadcast_procs[msg.uuid]
                msg.func_args = (aggregated_result,)

        if not transmitter:
            chan = await self.channel_store.get_chan(msg.transmitter)
            if chan:
                msg.flag = MessageFlag.UNABLE_TO_FIND_PROCESS
                info = "Unable to find process by message uuid"
                msg.set_error(RemoteError(info=info))
                chan.send(msg)
                self.logger.error(info)

        else:
            chan = await self.channel_store.get_chan(transmitter)
            if not chan:
                logging.error(f"Unable to send calculated result. Process {transmitter!r} is disconnected.")
            else:
                chan.send(msg)

    @with_debug_trace
    async def on_scheduler_error(self, msg: RpcMessage):
        # Dont care about RpcMessage uuid. Scheduler tasks are long running processes. Sheduler can report about error
        # multiple times.
        self.awaited_procs.pop(msg.uuid, None)
        chan = await self.channel_store.get_chan(msg.receiver)
        if not chan:
            logging.error(f"Unable to send calculated result. Process {msg.receiver!r} is disconnected.")
        else:
            msg.flag = MessageFlag.SUCCESS
            chan.send(msg)

    @with_debug_trace
    async def on_broadcast(self, msg: RpcMessage, process_name: str):

        return_result = msg.return_result
        if return_result:
            aggregated = dict()
            self.awaited_broadcast_procs[msg.uuid] = (msg.transmitter, aggregated)
        if msg.func_name in WELL_KNOWN_CALLBACKS:
            async for receiver, chan in self.channel_store.iterate():
                if receiver == process_name:
                    # Do not send broadcast RpcMessage to itself!
                    continue
                if return_result:
                    aggregated[receiver] = BROADCAST_RESULT_EMPTY
                chan.send(msg)
        else:
            data = search_remote_callback_in_mapping(
                self.controller_callback_mapping, msg.func_name, exclude=msg.transmitter, take_all=True
            )
            if data:
                for receiver, _ in data:
                    if receiver == process_name:
                        # Do not send broadcast RpcMessage to itself!
                        continue
                    # Take socket of destination process where function/method will be triggered.
                    chan = await self.channel_store.get_chan(receiver)
                    if chan:
                        if return_result:
                            aggregated[receiver] = BROADCAST_RESULT_EMPTY
                        chan.send(msg)

    async def on_reconnect(self, msg: ServiceMessage, sg: TaskGroup):
        transmitter = msg.transmitter
        if transmitter in self.awaited_procs:
            return

        for proc, aggregated_result in self.awaited_broadcast_procs.items():
            if transmitter == proc or transmitter in aggregated_result:
                return

        chan: ChannelPipe = await self.channel_store.get_chan(transmitter)
        if chan:
            # Give channel some time to reconnect (15 sec by default). If channel is not connected during timeout
            # option then delete channel from store.
            chan.register_fail_callback(self.on_channel_close, chan)
            sg.start_soon(chan.fire)
            chan.send(ServiceMessage(flag=MessageFlag.RECK_ACCEPT))
