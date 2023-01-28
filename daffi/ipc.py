import os
import sys
from itertools import chain
from logging import Logger
from inspect import iscoroutinefunction
from threading import Thread, Event
from typing import NoReturn, Dict, Union, Optional, Callable, Any, Tuple, AsyncGenerator

from anyio.from_thread import start_blocking_portal

from daffi.async_result import get_result_type
from daffi.components.controller import Controller
from daffi.components.node import Node
from daffi.exceptions import InitializationError, GlobalContextError, TimeoutError
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag
from daffi.signals import set_signal_handler
from daffi.utils.misc import (
    Period,
    search_remote_callback_in_mapping,
    resilent,
    ConditionEvent,
    async_library,
    iterable,
)
from daffi.utils.func_validation import pretty_callbacks
from daffi.utils.settings import LOCAL_CALLBACK_MAPPING, LOCAL_CLASS_CALLBACKS, WELL_KNOWN_CALLBACKS


class Ipc(Thread):
    def __init__(
        self,
        process_name: str,
        init_controller: bool,
        init_node: bool,
        global_terminate_event: Event,
        host: Optional[str] = None,
        port: Optional[int] = None,
        unix_sock_path: Optional[os.PathLike] = None,
        reconnect_freq: Optional[int] = None,
        logger: Logger = None,
    ):
        super().__init__()
        self.process_name = process_name
        self.init_controller = init_controller
        self.init_node = init_node
        self.global_terminate_event = global_terminate_event
        self.host = host
        self.port = port
        self.unix_sock_path = unix_sock_path
        self.reconnect_freq = reconnect_freq
        self.logger = logger
        self.async_backend = async_library()

        self.daemon = True
        self.global_condition_event = ConditionEvent()
        self.controller = self.node = None

        if not (self.init_controller or self.init_node):
            InitializationError("At least one of 'init_controller' or 'init_node' must be True.").fire()

        set_signal_handler(self.stop)

    @property
    def is_running(self) -> bool:
        """Return True if Node, Cotroller or Controller and Node were started successfully."""
        return self.global_condition_event.success

    @property
    def node_callback_mapping(self) -> Dict[str, Any]:
        """Return callback mapping for Node"""
        if self.node:
            return self.node.node_callback_mapping
        return dict()

    def controller_callback_mapping(self) -> Dict[str, Any]:
        """Return callback mapping for Controller."""
        if self.controller:
            return self.controller.controller_callback_mapping
        return dict()

    def wait(self) -> bool:
        return self.global_condition_event.wait()

    def call(
        self,
        func_name,
        args: Tuple,
        kwargs: Dict,
        timeout: Optional[Union[int, float]] = None,
        async_: Optional[bool] = False,
        eta: Optional[Union[int, float]] = 0,
        return_result: Optional[bool] = True,
        func_period: Optional[Period] = None,
        broadcast: Optional[bool] = False,
        inside_callback_context: Optional[bool] = False,
        stream: Optional[bool] = False,
    ):
        self._check_node()
        assert func_name is not None
        data = search_remote_callback_in_mapping(self.node.node_callback_mapping, func_name, exclude=self.process_name)
        if not data:
            if self.node.node_callback_mapping.get(self.process_name, {}).get(func_name):
                GlobalContextError(
                    f"function {func_name} not found on remote processes but found locally."
                    f" Communication between local node and the controller is prohibited."
                    f" You can always call the callback locally via regular python syntax as usual function."
                ).fire()
            else:
                # Get all callbacks without local
                available_callbacks = pretty_callbacks(
                    mapping=self.node.node_callback_mapping, exclude_proc=self.process_name, format="string"
                )
                GlobalContextError(
                    f"function {func_name!r} not found on remote.\n"
                    + f"Available registered callbacks:\n {available_callbacks}"
                    if available_callbacks
                    else ""
                ).fire()

        if stream:
            # Stream has special initialization and validation process.
            return self.stream(func_name, args)

        _, remote_callback = data
        remote_callback.validate_provided_arguments(*args, **kwargs)
        result = None

        msg = RpcMessage(
            flag=MessageFlag.REQUEST if not broadcast else MessageFlag.BROADCAST,
            transmitter=self.process_name,
            func_name=func_name,
            func_args=args,
            func_kwargs=kwargs,
            return_result=return_result,
            period=func_period,
        )
        result_class = get_result_type(inside_callback_context=inside_callback_context, is_period=bool(func_period))

        if return_result or func_period:
            result = result_class(
                func_name=func_name,
                uuid=msg.uuid,
            )
        if result:
            result._register()
        self.node.send_threadsave(msg, eta)

        if func_period:
            result._ipc = self
            result._scheduler_type = func_period.scheduler_type
            return result.get()

        if not async_ and return_result:
            result = result.get(timeout=timeout)
        return result

    def run(self) -> NoReturn:
        self.logger.info("Components initialization...")
        # TODO add windows support
        if sys.platform == "win32":
            backend_options = {}
        else:
            backend_options = {"use_uvloop": True}
        c_future = n_future = None
        with resilent(RuntimeError):
            with start_blocking_portal(backend="asyncio", backend_options=backend_options) as portal:
                if self.init_controller:
                    self.controller = Controller(
                        process_name=self.process_name,
                        host=self.host,
                        port=self.port,
                        global_terminate_event=self.global_terminate_event,
                    )

                    c_future, _ = portal.start_task(
                        self.controller.handle,
                        name=Controller.__class__.__name__,
                    )
                    self.port = getattr(self.controller, "port", None)
                    self.unix_sock_path = getattr(self.controller, "unix_sock_path", None)

                if self.init_node:
                    self.node = Node(
                        process_name=self.process_name,
                        host=self.host,
                        port=self.port,
                        reconnect_freq=self.reconnect_freq,
                        async_backend=self.async_backend,
                        global_terminate_event=self.global_terminate_event,
                    )

                    n_future, _ = portal.start_task(
                        self.node.handle,
                        name=Node.__class__.__name__,
                    )
                self.global_condition_event.mark_success()
                self.global_terminate_event.wait()

                # Wait controller and node to finish 'on_stop' lifecycle callbacks.
                if self.node:
                    self.node.stop(wait=True)
                if self.controller:
                    self.controller.stop(wait=True)

                # Wait pending futures to complete their tasks
                for future in filter(None, (c_future, n_future)):
                    if not future.done():
                        future.cancel()

        if not self.global_condition_event.success:
            self.global_condition_event.mark_fail()

        LOCAL_CALLBACK_MAPPING.clear()
        LOCAL_CLASS_CALLBACKS.clear()

    def stream(self, func_name, args: Tuple) -> NoReturn:
        if self.is_running:
            self._check_node()

            if len(args) != 1:
                InitializationError(
                    "Pass exactly 1 positional argument to initialize stream."
                    f" It can be list, tuple generator or any iterable. Provided args: {args}. Provided args len: {len(args)}"
                ).fire()
            stream_items = args[0]
            if not iterable(stream_items):
                if isinstance(stream_items, AsyncGenerator):
                    InitializationError(f"Async generators are not supported yet.").fire()
                InitializationError(
                    f"Stream support only iterable objects like lists, tuples, generators etc. "
                    f"You provided {stream_items} as argument."
                ).fire()

            stream_items = iter(stream_items)
            try:
                first_item = next(stream_items)
            except StopIteration:
                InitializationError("Stream is empty").fire()

            data = search_remote_callback_in_mapping(
                self.node.node_callback_mapping, func_name, exclude=self.process_name
            )
            _, remote_callback = data
            remote_callback.validate_provided_arguments(first_item)
            stream_items = chain([first_item], stream_items)

            msg = RpcMessage(
                flag=MessageFlag.INIT_STREAM,
                transmitter=self.process_name,
                func_name=func_name,
            )
            # Register result in order to obtain all available receivers
            result = self.node.send_and_register_result(func_name=func_name, msg=msg)

            self.logger.debug("Wait available stream receivers")
            receivers = result.get()
            if not receivers:
                InitializationError("Unable to find receivers for stream.").fire()

            # Register the same result second time to track stream errors (If happened)
            result = result._clone_and_register()
            stream_pair_was_closed = False
            with self.node.stream_store.request_multi_connection(
                receivers=receivers, msg_uuid=str(msg.uuid)
            ) as stream_pair_group:
                for stream_item in stream_items:
                    if stream_pair_group.closed:
                        stream_pair_was_closed = True
                        break
                    for msg in ServiceMessage.build_stream_message(data=stream_item):
                        stream_pair_group.send_threadsave(msg)
            # Wait all receivers to finish stream processing.
            self.logger.debug("Stream closed. Wait for confirmation from receivers...")

            result.get()
            if stream_pair_was_closed:
                raise GlobalContextError(
                    "Stream was closed unexpectedly. Seems payload is too big."
                    " Please adjust payload size or consider using unary exec modifiers FG, BG etc."
                )
            self.logger.debug("Confirmed.")

    def update_callbacks(self) -> NoReturn:
        if self.node:
            local_mapping_without_well_known_callbacks = {
                k: v.simplified() for k, v in LOCAL_CALLBACK_MAPPING.items() if k not in WELL_KNOWN_CALLBACKS
            }
            self.node.send_threadsave(
                ServiceMessage(
                    flag=MessageFlag.UPDATE_CALLBACKS,
                    transmitter=self.process_name,
                    data=local_mapping_without_well_known_callbacks,
                ),
                0,
            )

    def transfer_and_call(self, remote_process: str, func: Callable[..., Any], *args, **kwargs) -> Any:
        if self.is_running:
            self._check_node()
            if not callable(func):
                InitializationError("Provided func is not callable.").fire()

            if remote_process not in self.node.node_callback_mapping:
                InitializationError(f"Seems process {remote_process!r} is not running.").fire()

            func_name = "__async_transfer_and_call" if iscoroutinefunction(func) else "__transfer_and_call"
            msg = RpcMessage(
                flag=MessageFlag.REQUEST,
                transmitter=self.process_name,
                receiver=remote_process,
                func_name=func_name,
                func_args=(func, *args),
                func_kwargs=kwargs,
            )
            return self.node.send_and_register_result(func_name=func_name, msg=msg).get()

    async def async_transfer_and_call(self, remote_process: str, func: Callable[..., Any], *args, **kwargs) -> Any:
        if self.is_running:
            self._check_node()
            if not callable(func):
                InitializationError("Provided func is not callable.").fire()

            if remote_process not in self.node.node_callback_mapping:
                InitializationError(f"Seems process {remote_process!r} is not running.").fire()

            func_name = "__async_transfer_and_call" if iscoroutinefunction(func) else "__transfer_and_call"
            msg = RpcMessage(
                flag=MessageFlag.REQUEST,
                transmitter=self.process_name,
                receiver=remote_process,
                func_name=func_name,
                func_args=(func, *args),
                func_kwargs=kwargs,
            )
            return await self.node.send_and_register_result(func_name=func_name, msg=msg).get()

    def cancel_scheduler(self, remote_process: str, msg_uuid: str, func_name: Optional[str] = None) -> NoReturn:
        if self.is_running:
            self._check_node()
            msg = RpcMessage(
                flag=MessageFlag.REQUEST,
                transmitter=self.process_name,
                receiver=remote_process,
                func_name="__cancel_scheduled_task",
                func_args=(msg_uuid, self.process_name, func_name),
            )
            return self.node.send_and_register_result(func_name=func_name, msg=msg).get()

    def get_all_scheduled_tasks(self, remote_process: str):
        if self.is_running:
            self._check_node()
            func_name = "__get_all_period_tasks"
            msg = RpcMessage(
                flag=MessageFlag.REQUEST,
                transmitter=self.process_name,
                receiver=remote_process,
                func_name=func_name,
                func_args=(self.process_name,),
            )
            return self.node.send_and_register_result(func_name=func_name, msg=msg).get()

    def kill_all(self) -> NoReturn:
        if self.is_running:
            self._check_node()
            func_name = "__kill_all"
            msg = RpcMessage(
                flag=MessageFlag.STOP_REQUEST, transmitter=self.process_name, func_name=func_name, return_result=True
            )
            result = self.node.send_and_register_result(func_name=func_name, msg=msg)
            try:
                return result.get(timeout=10)
            except TimeoutError:
                pass

    def stop(self, *args, **kwargs):
        self.global_terminate_event.set()
        self.join()

    def _check_node(self) -> NoReturn:
        if not self.node:
            GlobalContextError(
                "Support for invoking remote calls is not enabled"
                " The node has not been initialized in the current process."
                " Make sure you passed 'init_node' = True in Global object."
            ).fire()
