import time
from inspect import iscoroutinefunction
from threading import Thread, Event
from typing import NoReturn, Dict, Union, Optional, Callable, Any, Tuple

from anyio import EndOfStream
from anyio.from_thread import start_blocking_portal

from dafi.async_result import AsyncResult, AwaitableAsyncResult, get_result_type
from dafi.backend import BackEndKeys, BackEndI
from dafi.components import ControllerStatus
from dafi.components.controller import Controller
from dafi.components.node import Node
from dafi.exceptions import InitializationError, GlobalContextError
from dafi.message import Message, MessageFlag
from dafi.signals import set_signal_handler
from dafi.utils.retry import resilent
from dafi.utils.misc import Period
from dafi.utils.custom_types import SchedulerTaskType
from dafi.utils.func_validation import pretty_callbacks
from dafi.utils.mappings import NODE_CALLBACK_MAPPING, search_remote_callback_in_mapping


class Ipc(Thread):
    def __init__(
        self,
        process_name: str,
        backend: BackEndI,
        init_controller: bool,
        init_node: bool,
        global_event: Event,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ):
        super().__init__()
        self.process_name = process_name
        self.backend = backend
        self.init_controller = init_controller
        self.init_node = init_node
        self.global_event = global_event
        self.host = host
        self.port = port

        self.daemon = True
        self.start_event = Event()
        self.controller = self.node = None

        if not (self.init_controller or self.init_node):
            raise InitializationError("At least one of 'init_controller' or 'init_node' must be True.")

        set_signal_handler(self.stop)

    @property
    def is_running(self) -> bool:
        return self.start_event.is_set()

    def wait(self) -> NoReturn:
        self.start_event.wait()

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
    ):
        if not self.node:
            raise GlobalContextError(
                "Support for invoking remote calls is not enabled"
                " The node has not been initialized in the current process."
                " Make sure you passed 'init_node' = True in Global object."
            )

        assert func_name is not None
        data = search_remote_callback_in_mapping(NODE_CALLBACK_MAPPING, func_name, exclude=self.process_name)
        if not data:
            if NODE_CALLBACK_MAPPING.get(self.process_name, {}).get(func_name):
                raise GlobalContextError(
                    f"function {func_name} not found on remote processes but found locally."
                    f" Communication between local node and the controller is prohibited."
                    f" You can always call the callback locally via regular python syntax as usual function."
                )
            else:
                # Get all callbacks without local
                available_callbacks = pretty_callbacks(exclude_proc=self.process_name, format="string")
                raise GlobalContextError(
                    f"function {func_name} not found on remote.\n"
                    + f"Available registered callbacks:\n {available_callbacks}"
                    if available_callbacks
                    else ""
                )

        _, remote_callback = data

        result = None
        msg = Message(
            flag=MessageFlag.REQUEST if not broadcast else MessageFlag.BROADCAST,
            transmitter=self.process_name,
            func_name=func_name,
            func_args=args,
            func_kwargs=kwargs,
            return_result=return_result,
            period=func_period,
        )

        if inside_callback_context:
            send_to_node_func = self.node.send
        else:
            send_to_node_func = self.node.send_threadsave
        result_class = get_result_type(inside_callback_context=inside_callback_context, is_period=bool(func_period))

        if return_result or func_period:
            result = result_class(
                func_name=func_name,
                uuid=msg.uuid,
            )
        self.node.register_result(result)
        send_to_node_func(msg.dumps(), eta)

        if func_period:
            result._ipc = self
            result._scheduler_type = func_period.scheduler_type
            return result.get()

        if not async_ and return_result:
            result = result.get(timeout=timeout)
        return result

    def run(self) -> NoReturn:
        if self.init_controller:
            controller_status = self.backend.read(BackEndKeys.CONTROLLER_STATUS)
            controller_hc_ts = self.backend.read(BackEndKeys.CONTROLLER_HEALTHCHECK_TS)

            if (
                controller_status != ControllerStatus.RUNNING
                or not controller_hc_ts
                or (time.time() - controller_hc_ts) > Controller.TIMEOUT
            ):
                self.backend.delete_key(BackEndKeys.CONTROLLER_HEALTHCHECK_TS)
                self.backend.delete_key(BackEndKeys.CONTROLLER_STATUS)
            try:
                self.backend.write_if_not_exist(BackEndKeys.CONTROLLER_STATUS, ControllerStatus.RUNNING)
                self.controller = Controller(self.process_name, self.backend, self.host, self.port)
            except ValueError:
                # Controller has been initialized by other process
                ...
        if not self.controller and self.init_controller:
            raise InitializationError("Unable to initialize controller."
                                      " Seems controller already running in another process."
                                      "Terminate another controller or set 'init_controller=False' for current process")


        if self.init_node:
            self.node = Node(self.process_name, self.backend, self.host, self.port)

        if self.node or self.controller:
            c_future = n_future = None

            with resilent((RuntimeError, EndOfStream)):
                with start_blocking_portal() as portal:
                    if self.controller:
                        c_future, _ = portal.start_task(
                            self.controller.handle,
                            self.global_event,
                            name=Controller.__class__.__name__,
                        )

                    if self.node:
                        n_future, _ = portal.start_task(
                            self.node.handle,
                            self.global_event,
                            name=Node.__class__.__name__,
                        )

                    self.start_event.set()
                    self.global_event.wait()
                    # Wait pending futures to complete their tasks
                    time.sleep(1.5)
                    for future in filter(None, (c_future, n_future)):
                        if not future.done():
                            future.cancel()

    def update_callbacks(self, func_info: Dict[str, "RemoteCallback"]) -> NoReturn:
        self.node.send(
            Message(
                flag=MessageFlag.UPDATE_CALLBACKS,
                transmitter=self.process_name,
                func_args=(func_info,),
            ).dumps()
        )

    def transfer_and_call(self, remote_process: str, func: Callable[..., Any], *args, **kwargs) -> Any:
        if not callable(func):
            raise InitializationError("Provided func is not callable.")

        if remote_process not in NODE_CALLBACK_MAPPING:
            raise InitializationError(f"Seems process {remote_process!r} is not running.")

        func_name = "__async_transfer_and_call" if iscoroutinefunction(func) else "__transfer_and_call"
        msg = Message(
            flag=MessageFlag.REQUEST,
            transmitter=self.process_name,
            receiver=remote_process,
            func_name=func_name,
            func_args=(func, *args),
            func_kwargs=kwargs,
        )
        result = AsyncResult(
            func_name=func_name,
            uuid=msg.uuid,
        )
        self.node.register_result(result)

        self.node.send_threadsave(msg.dumps(), 0)
        return result.get()

    async def async_transfer_and_call(self, remote_process: str, func: Callable[..., Any], *args, **kwargs) -> Any:
        if not callable(func):
            raise InitializationError("Provided func is not callable.")

        if remote_process not in NODE_CALLBACK_MAPPING:
            raise InitializationError(f"Seems process {remote_process!r} is not running.")

        func_name = "__async_transfer_and_call" if iscoroutinefunction(func) else "__transfer_and_call"
        msg = Message(
            flag=MessageFlag.REQUEST,
            transmitter=self.process_name,
            receiver=remote_process,
            func_name=func_name,
            func_args=(func, *args),
            func_kwargs=kwargs,
        )
        result = AwaitableAsyncResult(
            func_name=func_name,
            uuid=msg.uuid,
        )
        self.node.register_result(result)

        self.node.send(msg.dumps(), 0)
        return await result.get()

    def cancel_scheduler(
        self, remote_process: str, scheduler_type: SchedulerTaskType, msg_uuid: str, func_name: str
    ) -> NoReturn:
        msg = Message(
            flag=MessageFlag.REQUEST,
            transmitter=self.process_name,
            receiver=remote_process,
            func_name="__cancel_scheduled_task",
            func_args=(scheduler_type, msg_uuid, func_name),
            return_result=False,
        )
        self.node.send(msg.dumps(), 0)

    def stop(self, *args, **kwargs):
        if self.controller:
            self.backend.delete_key(BackEndKeys.CONTROLLER_HEALTHCHECK_TS)
            self.backend.write(BackEndKeys.CONTROLLER_STATUS, ControllerStatus.UNAVAILABLE)
            msg = Message(
                flag=MessageFlag.BROADCAST,
                transmitter=self.process_name,
                func_name="__on_controller_stop",
                return_result=False,
            )
            self.node.send(msg.dumps(), 0)
            time.sleep(0.1)

        self.global_event.set()
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            AsyncResult._awaited_results[msg_uuid] = None
            if isinstance(ares, AsyncResult):
                ares._ready.set()
        if self.controller:
            self.backend.delete_key(BackEndKeys.CONTROLLER_HEALTHCHECK_TS)
            self.backend.write(BackEndKeys.CONTROLLER_STATUS, ControllerStatus.UNAVAILABLE)
