import os
import sys
import time
from itertools import chain, cycle
from logging import Logger
from inspect import iscoroutinefunction
from threading import Thread, Event
from typing import NoReturn, Dict, Union, Optional, Callable, Any, Tuple, AsyncGenerator

from anyio.from_thread import start_blocking_portal

from daffi.utils import colors
from daffi.settings import DEBUG
from daffi.async_result import get_result_type, AsyncResult
from daffi.components.controller import Controller
from daffi.components.node import Node
from daffi.components.operations.task_waiter import TaskWaiter
from daffi.exceptions import InitializationError, GlobalContextError
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag
from daffi.signals import set_signal_handler, SIGNALS_TO_NAMES_DICT
from daffi.utils.misc import (
    Period,
    search_remote_callback_in_mapping,
    resilent,
    ConditionEvent,
    async_library,
    iterable,
)
from daffi.utils.func_validation import pretty_callbacks
from daffi.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS, clear_method_type_stores


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
        self.logger = logger
        self.async_backend = async_library()

        self.daemon = True
        self.global_condition_event = ConditionEvent()
        self.controller = self.node = None
        self.task_waiter = TaskWaiter(process_name)

        AsyncResult._ipc = self

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

    @property
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
        stream: Optional[bool] = False,
    ):
        self._check_node()
        assert func_name is not None
        data = search_remote_callback_in_mapping(self.node.node_callback_mapping, func_name, exclude=self.process_name)
        if not data:
            # Get all callbacks without local
            available_callbacks = pretty_callbacks(
                mapping=self.node.node_callback_mapping, exclude_proc=self.process_name, format="string"
            )
            msg = (
                f"function {func_name!r} not found on remote.\n Available registered callbacks:\n"
                + f"{available_callbacks}"
                if available_callbacks
                else "No available callbacks found on remotes"
            )
            GlobalContextError(msg).fire()

        if stream:
            # Stream has special initialization and validation process.
            if async_:
                # Start stream process in background without blocking main process execution.
                stream_thread = Thread(target=self.stream, args=(func_name, args), daemon=True)
                return stream_thread.start()
            # Block main process execution until stream is completed.
            return self.stream(func_name, args)

        _, remote_callback = data
        remote_callback.validate_provided_arguments(*args, **kwargs)
        result = None

        if remote_callback.is_generator and (broadcast or not return_result or func_period or async_):
            InitializationError("Remote callback which are generators works only with FG execution modifier!")

        wait_in_task_waiter = async_ and not return_result and not func_period

        msg = RpcMessage(
            flag=MessageFlag.REQUEST if not broadcast else MessageFlag.BROADCAST,
            transmitter=self.process_name,
            func_name=func_name,
            func_args=args,
            func_kwargs=kwargs,
            return_result=return_result or wait_in_task_waiter,
            period=func_period,
            timeout=timeout or 0,
        )
        result_class = get_result_type(
            is_period=bool(func_period),
            is_generator=remote_callback.is_generator,
        )

        if return_result or func_period:
            result = result_class(msg=msg)
        if result:
            result._timeout = timeout
            result._register()

        if wait_in_task_waiter:
            # Wait result in background.
            # such an waiting is purely informational in nature to inform the user about the error.
            # It make sense only when user is not expecting result to be returned.
            self.task_waiter.register_result(msg)

        self.node.send_threadsave(msg, eta)

        if func_period:
            result._scheduler_type = func_period.scheduler_type
            return result.get()

        if not async_ and return_result:
            result = result.get(timeout=timeout)
        return result

    def run(self) -> NoReturn:
        if DEBUG:
            self.logger.info(f"{colors.yellow('DEBUG')} mode enabled.")
        self.logger.info("Components initialization...")

        # TODO add windows support
        if sys.platform == "win32":
            backend_options = {}
        else:
            backend_options = {"use_uvloop": True}
        c_future = n_future = tw_future = None
        with resilent(RuntimeError):
            Controller.components.clear()
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
                        async_backend=self.async_backend,
                        global_terminate_event=self.global_terminate_event,
                    )

                    n_future, _ = portal.start_task(
                        self.node.handle,
                        name=Node.__class__.__name__,
                    )

                    tw_future, _ = portal.start_task(
                        self.task_waiter,
                        name=self.task_waiter.__class__.__name__,
                    )

                self.global_condition_event.mark_success()
                self.global_terminate_event.wait()

                # Wait controller and node to finish 'on_stop' lifecycle callbacks.
                if self.node:
                    self.node.stop(wait=True)
                    self.task_waiter.stop()
                if self.controller:
                    self.controller.stop(wait=True)

                # Wait pending futures to complete their tasks
                for future in filter(None, (c_future, n_future, tw_future)):
                    if not future.done():
                        future.cancel()

        if not self.global_condition_event.success:
            self.global_condition_event.mark_fail()

        clear_method_type_stores()

    def stream(self, func_name, args: Tuple) -> NoReturn:
        from daffi.registry._fetcher import Args

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
            if remote_callback.is_generator:
                InitializationError(
                    f"Stream don't work with remote callback which are generators."
                    f" Check {remote_callback.alias} to fix this issue."
                ).fire()

            args, kwargs = Args._aggregate_args(args=first_item)
            remote_callback.validate_provided_arguments(*args, **kwargs)
            stream_items = chain([first_item], stream_items)

            msg = RpcMessage(
                flag=MessageFlag.INIT_STREAM,
                transmitter=self.process_name,
                func_name=func_name,
            )
            # Register result in order to obtain all available receivers
            result = self.node.send_and_register_result(msg=msg)

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
                    # Convert stream item to args, kwargs
                    args, kwargs = Args._aggregate_args(args=stream_item)
                    for msg in ServiceMessage.build_stream_message(data=(args, kwargs)):
                        stream_pair_group.send_threadsave(msg)
            # Wait all receivers to finish stream processing.
            self.logger.debug("Stream closed. Wait for confirmation from receivers...")

            result.get()
            if stream_pair_was_closed:
                raise GlobalContextError(
                    "Stream was closed unexpectedly. Seems payload is too big."
                    " Please adjust payload size or consider using unary exec modifiers FG, BG etc."
                )

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
            return self.node.send_and_register_result(msg=msg).get()

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
            return await self.node.send_and_register_result(msg=msg).get()

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
            return self.node.send_and_register_result(msg=msg).get()

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
            return self.node.send_and_register_result(msg=msg).get()

    def kill_all(self) -> NoReturn:
        if self.is_running:
            self._check_node()
            func_name = "__kill_all"
            msg = RpcMessage(
                flag=MessageFlag.STOP_REQUEST, transmitter=self.process_name, func_name=func_name, return_result=False
            )
            self.node.send_threadsave(msg, 0)
            for i in cycle(range(1, 51)):
                if not (diff := {k for k in self.controller_callback_mapping} - {self.process_name}):
                    break
                if i == 25:
                    self.node.send_threadsave(msg, 0)
                elif i == 50:
                    self.logger.error(f"Node(s): {diff} are still pending after after kill signal.")
                time.sleep(0.5)

    def stop(self, *args, **kwargs):
        if args:
            sig_num = args[0]
            sig_name = SIGNALS_TO_NAMES_DICT[sig_num]
            self.logger.warning(f"Terminated by signal {sig_name!r}")

        self.global_terminate_event.set()
        self.join()

    def _check_node(self) -> NoReturn:
        if not self.node:
            GlobalContextError(
                "Support for invoking remote calls is not enabled"
                " The node has not been initialized in the current process."
                " Make sure you passed 'init_node' = True in Global object."
            ).fire()

    def _wait_all_bg_tasks(self):
        self.task_waiter.wait_all_results()
