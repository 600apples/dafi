import logging
from itertools import chain
from collections import defaultdict
from typing import Dict, Tuple, Any

from anyio import sleep
from anyio.abc import TaskGroup

from daffi.utils.settings import WELL_KNOWN_CALLBACKS
from daffi.utils.custom_types import K, GlobalCallback
from daffi.utils.misc import search_remote_callback_in_mapping
from daffi.components.operations.streams_store import StreamPairStore
from daffi.components.operations.channel_store import ChannelStore, ChannelPipe, FreezableQueue
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag, RemoteError


RESULT_EMPTY = object()


class ControllerOperations:
    """Controller operations specification object"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.channel_store = ChannelStore()
        self.stream_store = StreamPairStore()
        self.controller_callback_mapping: Dict[K, Dict[K, GlobalCallback]] = dict()
        self.awaited_procs: Dict[str, Tuple[str, str]] = dict()
        self.awaited_broadcast_procs: Dict[str, Tuple[str, Dict[str, Any]]] = dict()
        self.awaited_stream_procs: Dict[str, Tuple[str, Dict[str, Any]]] = dict()

    async def wait_all_requests_done(self):
        while self.awaited_procs or self.awaited_broadcast_procs or self.awaited_stream_procs:
            await sleep(0.1)

    async def wait_all_channels_unlocked(self):
        while any(c.locked for c in self.channel_store.values()):
            await sleep(0.1)

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
        awaited_trmrs_for_single_message = defaultdict(list)
        [
            awaited_trmrs_for_single_message[trans].append(uuid)
            for uuid, (trans, rec) in self.awaited_procs.items()
            if rec == del_proc
        ]

        async for proc, chan in self.channel_store.iterate():
            # Send updated self.controller_callback_mapping to all processes including transmitter process.
            await chan.send(
                ServiceMessage(
                    flag=MessageFlag.UPDATE_CALLBACKS,
                    transmitter=del_proc,
                    receiver=proc,
                    data=self.controller_callback_mapping,
                )
            )
            awaited_messages = awaited_trmrs_for_single_message.get(proc)
            if awaited_messages:
                for msg_uuid in awaited_messages:
                    await chan.send(
                        RpcMessage(
                            flag=MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY,
                            transmitter=del_proc,
                            receiver=proc,
                            uuid=msg_uuid,
                            error=RemoteError(info=f"Remote process {del_proc} stopped unexpectedly."),
                        )
                    )

        # Find receivers that awaiting result from deleted process (broadcast messages)
        awaited_trmrs_for_broadcast = defaultdict(list)
        [
            awaited_trmrs_for_broadcast[trans].append(uuid)
            for uuid, (trans, agg_msg) in self.awaited_broadcast_procs.items()
            if del_proc in agg_msg
        ]

        async for proc, chan in self.channel_store.iterate():
            awaited_messages = awaited_trmrs_for_broadcast.get(proc)
            if awaited_messages:
                for msg_uuid in awaited_messages:
                    _, aggregated_result = self.awaited_broadcast_procs[msg_uuid]
                    aggregated_result[del_proc] = RemoteError(info=f"Remote process {del_proc} stopped unexpectedly.")
                    if all(res != RESULT_EMPTY for res in aggregated_result.values()):
                        await chan.send(
                            RpcMessage(
                                flag=MessageFlag.SUCCESS,
                                receiver=proc,
                                uuid=msg_uuid,
                                func_args=(aggregated_result,),
                            )
                        )
                        self.awaited_broadcast_procs.pop(msg_uuid, None)

            # Stop streams if exist
            awaited_trmrs_for_stream = defaultdict(list)
            [
                awaited_trmrs_for_stream[trans].append(uuid)
                for uuid, (trans, agg_msg) in self.awaited_stream_procs.items()
                if del_proc in agg_msg
            ]

            async for proc, chan in self.channel_store.iterate():
                awaited_messages = awaited_trmrs_for_stream.get(proc)
                if awaited_messages:
                    for msg_uuid in awaited_messages:
                        _, aggregated_result = self.awaited_stream_procs[msg_uuid]
                        err = RemoteError(info=f"Remote process {del_proc} stopped unexpectedly.")
                        await chan.send(
                            RpcMessage(
                                flag=MessageFlag.STREAM_ERROR,
                                receiver=proc,
                                transmitter=del_proc,
                                uuid=msg_uuid,
                                error=err,
                            )
                        )
                        self.awaited_broadcast_procs.pop(msg_uuid, None)

    async def on_handshake(self, msg: ServiceMessage, channel: ChannelPipe):
        msg.loads()
        self.controller_callback_mapping[msg.transmitter] = msg.data
        msg.set_data(self.controller_callback_mapping)

        was_locked = await self.channel_store.add_channel(channel=channel, process_name=msg.transmitter)
        if not was_locked:
            async for proc, chan in self.channel_store.iterate():
                # Send updated self.controller_callback_mapping to all processes except transmitter process.
                await chan.send(msg.copy(transmitter=msg.transmitter, receiver=proc))

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
                await chan.send(msg)

                if msg.period and trans_chan:
                    # Return back to transmitter info that scheduled task was accepted.
                    await trans_chan.send(
                        msg.copy(flag=MessageFlag.SCHEDULER_ACCEPT, transmitter=receiver, receiver=transmitter)
                    )

        else:
            msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE
            info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
            msg.set_error(RemoteError(info=info))
            if trans_chan:
                await trans_chan.send(msg)
            self.logger.error(info)

    async def on_success(self, msg: RpcMessage):
        transmitter = None

        if msg.uuid in self.awaited_procs:
            transmitter, _ = self.awaited_procs.pop(msg.uuid)

        # ------------------------
        # Broadcast
        elif msg.uuid in self.awaited_broadcast_procs:
            msg.loads()
            transmitter, aggregated_result = self.awaited_broadcast_procs[msg.uuid]
            aggregated_result[msg.transmitter] = msg.error or msg.func_args[0]
            if any(res == RESULT_EMPTY for res in aggregated_result.values()):
                # Not all results aggregated yet
                return
            else:
                del self.awaited_broadcast_procs[msg.uuid]
                msg.func_args = (aggregated_result,)

        # ------------------------
        # Stream
        elif msg.uuid in self.awaited_stream_procs:
            msg.loads()
            transmitter, aggregated_result = self.awaited_stream_procs[msg.uuid]

            if msg.error:
                # Stop stream if any of receivers failed.
                del self.awaited_stream_procs[msg.uuid]
                msg = msg.copy(flag=MessageFlag.STREAM_ERROR)

            else:
                aggregated_result[msg.transmitter] = msg.func_args[0]
                if any(res == RESULT_EMPTY for res in aggregated_result.values()):
                    # Not all results aggregated yet
                    return
                else:
                    del self.awaited_stream_procs[msg.uuid]
                    # Return result if last stream iteration of each receiver.
                    # It might be helpful where result of
                    # executable remote callback is some aggregated value that is populated during the stream
                    msg.func_args = (aggregated_result,)

        if not transmitter:
            chan = await self.channel_store.get_chan(msg.transmitter)
            if chan:
                msg.flag = MessageFlag.UNABLE_TO_FIND_PROCESS
                info = "Unable to find process by message uuid"
                msg.set_error(RemoteError(info=info))
                await chan.send(msg)
                self.logger.error(info)

        else:
            chan = await self.channel_store.get_chan(transmitter)
            if not chan:
                logging.error(f"Unable to send calculated result. Process {transmitter!r}.")
            else:
                await chan.send(msg)

    async def on_scheduler_error(self, msg: RpcMessage):
        # Dont care about RpcMessage uuid. Scheduler tasks are long running processes. Sheduler can report about error
        # multiple times.
        self.awaited_procs.pop(msg.uuid, None)
        chan = await self.channel_store.get_chan(msg.receiver)
        if not chan:
            logging.error(f"Unable to send calculated result. Process {msg.receiver!r} is disconnected.")
        else:
            msg.flag = MessageFlag.SUCCESS
            await chan.send(msg)

    async def on_broadcast(self, msg: RpcMessage, process_name: str):

        transmitter = msg.transmitter
        trans_chan = await self.channel_store.get_chan(transmitter)

        return_result = msg.return_result
        if return_result:
            aggregated = dict()
            self.awaited_broadcast_procs[msg.uuid] = (transmitter, aggregated)
        if msg.func_name in WELL_KNOWN_CALLBACKS:
            async for receiver, chan in self.channel_store.iterate():
                if receiver == process_name:
                    # Do not send broadcast RpcMessage to itself!
                    continue
                if return_result:
                    aggregated[receiver] = RESULT_EMPTY
                await chan.send(msg.copy(receiver=receiver))
        else:
            data = search_remote_callback_in_mapping(
                self.controller_callback_mapping, msg.func_name, exclude=transmitter, take_all=True
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
                            aggregated[receiver] = RESULT_EMPTY
                        await chan.send(msg.copy(receiver=receiver))
            else:
                # TODO test when broadcast request sent but no available receivers
                info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
                self.awaited_broadcast_procs.pop(msg.uuid, None)
                if return_result:
                    msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE

                    msg.set_error(RemoteError(info=info))
                    if trans_chan:
                        await trans_chan.send(msg)
                self.logger.error(info)

    async def on_reconnect(self, msg: ServiceMessage, sg: TaskGroup):
        transmitter = msg.transmitter
        if transmitter in self.awaited_procs:
            return

        for proc, aggregated_result in chain(self.awaited_broadcast_procs.items(), self.awaited_stream_procs.items()):
            if transmitter == proc or transmitter in aggregated_result:
                return

        chan: ChannelPipe = await self.channel_store.get_chan(transmitter)
        if chan:
            # Give channel some time to reconnect (15 sec by default). If channel is not connected during timeout
            # option then delete channel from store.
            chan.register_fail_callback(self.on_channel_close, chan, transmitter)
            sg.start_soon(chan.fire)
            await chan.send(ServiceMessage(flag=MessageFlag.RECK_ACCEPT))

    async def on_stream_init(self, msg: RpcMessage):
        transmitter = msg.transmitter
        trans_chan = await self.channel_store.get_chan(transmitter)

        self.logger.debug(f"Received stream request from transmitter: {transmitter}")

        aggregated = dict()
        self.awaited_stream_procs[msg.uuid] = (transmitter, aggregated)

        data = search_remote_callback_in_mapping(
            self.controller_callback_mapping, msg.func_name, exclude=msg.transmitter, take_all=True
        )
        if data:
            for receiver, _ in data:
                chan = await self.channel_store.get_chan(receiver)
                if chan:
                    await chan.send(msg.copy(receiver=receiver))
                    aggregated[receiver] = RESULT_EMPTY
            await trans_chan.send(msg.copy(flag=MessageFlag.SUCCESS, func_args=(list(aggregated),), data=None))
            self.logger.debug(f"Found next stream receiver candidates: {list(aggregated)}")
        else:
            info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
            self.awaited_stream_procs.pop(msg.uuid, None)
            msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE

            msg.set_error(RemoteError(info=info))
            if trans_chan:
                await trans_chan.send(msg)
            self.logger.error(info)

    async def on_stream_throttle(self, msg: ServiceMessage):
        chan = await self.channel_store.get_chan(msg.receiver)
        if chan:
            await chan.send(msg)
