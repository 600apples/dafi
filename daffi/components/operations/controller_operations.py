import logging
import asyncio
from typing import Dict

from anyio import sleep

from daffi.settings import WELL_KNOWN_CALLBACKS, DEBUG
from daffi.utils.custom_types import K, GlobalCallback
from daffi.utils.misc import search_remote_callback_in_mapping, call_after
from daffi.components.operations.streams_store import StreamPairStore
from daffi.components.operations.channel_store import ChannelStore, ChannelPipe, FreezableQueue
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag, RemoteError
from daffi.components.operations.ttl_store import OneToOneCallStore, OneToManyCallStore


RESULT_EMPTY = object()


class ControllerOperations:
    """Controller operations specification object"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.channel_store = ChannelStore()
        self.stream_store = StreamPairStore()
        self.controller_callback_mapping: Dict[K, Dict[K, GlobalCallback]] = dict()
        self.closed_procs: Dict[str, asyncio.Task] = dict()

        self.awaited_procs = OneToOneCallStore()
        self.awaited_broadcast_procs = OneToManyCallStore()
        self.awaited_stream_procs = OneToManyCallStore()

    async def wait_all_requests_done(self):
        while self.awaited_procs or self.awaited_broadcast_procs or self.awaited_stream_procs:
            await sleep(0.1)

    async def on_controller_stopped(self, proc_name):
        async for proc, chan in self.channel_store.iterate():
            if proc != proc_name:
                # Send updated self.controller_callback_mapping to all processes except transmitter process.
                await chan.send(ServiceMessage(flag=MessageFlag.CONTROLLER_STOPPED_UNEXPECTEDLY, receiver=proc))

    async def on_channel_close(self, channel: ChannelPipe, process_identificator: str):
        self.logger.debug(f"Channel {channel} (process={process_identificator}) is locked before closing")
        channel.lock()
        # Give 5 seconds to reconnect. If process unable to recconect within 5 seconds then channel will be
        # deleted and all other processes will be notified channel is not available anymore.
        if process_identificator not in self.closed_procs:
            self.closed_procs[process_identificator] = await call_after(
                5, self._on_channel_close, channel=channel, process_identificator=process_identificator
            )

    async def on_all_channels_close(self):
        async for proc, chan in self.channel_store.iterate():
            await self._on_channel_close(chan, chan.ident)

    async def _on_channel_close(self, channel: ChannelPipe, process_identificator: str):
        self.closed_procs.pop(process_identificator, None)
        del_proc = await self.channel_store.find_process_name_by_channel(channel)
        await self.channel_store.delete_channel(del_proc)
        self.controller_callback_mapping.pop(del_proc, None)
        FreezableQueue.factory_remove(process_identificator)

        # Delete RpcMessage pointers that deleted process expects to obtain.
        awaited_trmrs_for_single_message = self.awaited_procs.on_delete(del_proc)
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
        awaited_trmrs_for_broadcast = self.awaited_broadcast_procs.on_delete(del_proc)
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
            awaited_trmrs_for_stream = self.awaited_stream_procs.on_delete(del_proc)
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

    async def on_handshake(self, msg: ServiceMessage, channel: ChannelPipe, process_identificator: str):
        msg.loads()
        transmitter = msg.transmitter
        self.controller_callback_mapping[transmitter] = msg.data

        self.logger.debug(
            f"received {'handshake' if msg.flag == MessageFlag.HANDSHAKE else 'update callback'}"
            f" event from {msg.transmitter!r}. Available callbacks: {list(msg.data)}"
        )

        msg.set_data(self.controller_callback_mapping)
        # Send handshake (callbacks info) to all connected nodes if it is initial handshake (not re-connection)
        await self.channel_store.add_channel(channel=channel, process_name=msg.transmitter)
        if not (on_close_chan_task := self.closed_procs.pop(process_identificator, None)):
            async for proc, chan in self.channel_store.iterate():
                # Send updated self.controller_callback_mapping to all processes.
                await chan.send(msg.copy(transmitter=msg.transmitter, receiver=proc))
        else:
            # Process re-connected within 5 seconds. Cancel on_channel_close task.
            on_close_chan_task.cancel()
            await channel.send(msg.copy(transmitter=msg.transmitter, receiver=msg.receiver))

    async def on_request(self, msg: RpcMessage):

        receiver = msg.receiver
        transmitter = msg.transmitter
        trans_chan = await self.channel_store.get_chan(transmitter)

        if not receiver:
            if data := search_remote_callback_in_mapping(
                self.controller_callback_mapping, msg.func_name, exclude=transmitter
            ):
                receiver, _ = data

        if receiver and receiver in self.controller_callback_mapping:
            msg.receiver = receiver
            # Take channel of destination process where function/method will be triggered.
            if chan := await self.channel_store.get_chan(receiver):
                if msg.return_result:
                    # Assign transmitter to RpcMessage id in order to redirect result after processing.
                    await self.awaited_procs.set(key=msg.uuid, value=(transmitter, receiver), ttl=msg.timeout)

                await chan.send(msg)
                if msg.period and trans_chan:
                    # Return back to transmitter info that scheduled task was accepted.
                    await trans_chan.send(
                        msg.copy(flag=MessageFlag.SCHEDULER_ACCEPT, transmitter=receiver, receiver=transmitter)
                    )
            else:
                msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE
                info = f"Unable to execute {msg.func_name!r}. Remote channel is closed."
                msg.set_error(RemoteError(info=info))
                if trans_chan:
                    await trans_chan.send(msg)
                self.logger.error(info)
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
            if msg.completed:
                # One-off message. We can delete metadata from awaited procs
                transmitter, _ = self.awaited_procs.pop(msg.uuid)
            else:
                # Message from remote generator. We expect to receive additional messages.
                transmitter, _ = self.awaited_procs.get(msg.uuid)

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
            if chan := await self.channel_store.get_chan(msg.transmitter):
                msg.flag = MessageFlag.UNABLE_TO_FIND_PROCESS
                info = f"Unable to find process by message uuid."
                if DEBUG:
                    info += (
                        f" message: {msg}.\n"
                        f"Awaited stream entries: { self.awaited_stream_procs}.\n"
                        f"Awaited broadcast entries: {self.awaited_broadcast_procs}.\n"
                        f" Awaited one-to-one entries: {self.awaited_procs}."
                    )
                msg.set_error(RemoteError(info=info))
                await chan.send(msg)
                self.logger.error(info)

        else:
            if chan := await self.channel_store.get_chan(transmitter):
                await chan.send(msg)
            else:
                logging.error(f"Unable to send calculated result. Process {transmitter!r}.")

    async def on_scheduler_error(self, msg: RpcMessage):
        # Dont care about RpcMessage uuid. Scheduler tasks are long running processes. Sheduler can report about error
        # multiple times.
        self.awaited_procs.pop(msg.uuid, None)
        if not (chan := await self.channel_store.get_chan(msg.receiver)):
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
            await self.awaited_broadcast_procs.set(key=msg.uuid, value=(transmitter, aggregated), ttl=msg.timeout)

        if msg.func_name in WELL_KNOWN_CALLBACKS:
            async for receiver, chan in self.channel_store.iterate():
                if receiver == process_name:
                    # Do not send broadcast RpcMessage to itself!
                    continue
                if return_result:
                    aggregated[receiver] = RESULT_EMPTY
                await chan.send(msg.copy(receiver=receiver))
        else:
            if data := search_remote_callback_in_mapping(
                self.controller_callback_mapping, msg.func_name, exclude=transmitter, take_all=True
            ):
                for receiver, _ in data:
                    # Take socket of destination process where function/method will be triggered.
                    if chan := await self.channel_store.get_chan(receiver):
                        if return_result:
                            aggregated[receiver] = RESULT_EMPTY
                        await chan.send(msg.copy(receiver=receiver))
                self.logger.debug(f"receivers: {sorted(list(aggregated))} found for callback {msg.func_name}")

            else:
                info = f"Unable to find remote process candidate to execute {msg.func_name!r}"
                self.awaited_broadcast_procs.pop(msg.uuid, None)
                if return_result:
                    msg.flag = MessageFlag.UNABLE_TO_FIND_CANDIDATE

                    msg.set_error(RemoteError(info=info))
                    if trans_chan:
                        await trans_chan.send(msg)
                self.logger.error(info)

    async def on_stream_init(self, msg: RpcMessage):
        transmitter = msg.transmitter
        trans_chan = await self.channel_store.get_chan(transmitter)

        self.logger.debug(f"Received stream request from transmitter: {transmitter}")

        aggregated = dict()
        await self.awaited_stream_procs.set(key=msg.uuid, value=(transmitter, aggregated), ttl=msg.timeout)

        if data := search_remote_callback_in_mapping(
            self.controller_callback_mapping, msg.func_name, exclude=msg.transmitter, take_all=True
        ):
            for receiver, _ in data:
                if chan := await self.channel_store.get_chan(receiver):
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
        if chan := await self.channel_store.get_chan(msg.receiver):
            await chan.send(msg)

    async def on_receiver_error(self, msg: ServiceMessage):
        """
        MessageFlag.RECEIVER_ERROR
        Give extra information about why error happened.
        At this moment this method is not used since there is no implementation on Node side.
        But it might be helpful for debugging. That is why it is here.
        """
        uuid = msg.uuid
        if DEBUG:
            info = f"Error reason for message {uuid!r} intended for transmitter: {msg.transmitter}\n"
        else:
            info = ""

        if uuid in self.awaited_procs:
            _, receiver = self.awaited_procs.pop(uuid)
            if DEBUG:
                info += f"{'-':>10} message represents one-to-one operation\n"
                info += f"{'-':>10} message receiver = {receiver}\n"
                if await self.channel_store.get_chan(receiver):
                    info += f"{'-':>10} receiver channel found\n"
                else:
                    info += f"{'-':>10} receiver channel not found\n"
            del self.awaited_procs[uuid]

        # ------------------------
        # Broadcast
        elif uuid in self.awaited_broadcast_procs:
            if DEBUG:
                info += f"{'-':>10} message represents broadcast operation\n"
                _, aggregated_result = self.awaited_broadcast_procs[uuid]
                info += f"{'-':>10} message receivers = {', '.join(aggregated_result)}\n"
                for receiver, result in aggregated_result.items():
                    if result != RESULT_EMPTY:
                        info += f"{'-':>10} receiver {receiver!r} already delivered result to controller\n"
                    else:
                        if await self.channel_store.get_chan(receiver):
                            info += f"{'-':>10} receiver {receiver!r} found but not delivered result!\n"
                        else:
                            info += f"{'-':>10} receiver {receiver!r} not found\n"
            del self.awaited_broadcast_procs[uuid]

        else:
            info += f"{'-':>10} message not found! (timeout or canceled)\n"

        if info:
            self.logger.error(info)
