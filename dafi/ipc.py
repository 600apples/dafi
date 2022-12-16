import time
from anyio.from_thread import start_blocking_portal

from threading import Thread, Event
from dafi.backend import BackEndKeys, BackEnd
from dafi.exceptions import InitializationError, GlobalContextError
from dafi.components import MasterStatus
from dafi.components.unix import UnixSocketMaster, UnixSocketNode
from dafi.signals import set_signal_handler
from dafi.messaging import Message, MessageFlag


class Ipc:

    def __init__(self, process_name: str, backend: BackEnd, init_master: bool, init_node: bool):
        self.start_event = Event()
        self.stop_event = Event()
        self.worker_thread = None
        self.master = self.node = None

        self.process_name = process_name
        self.backend = backend
        self.init_master = init_master
        self.init_node = init_node

        if not (self.init_master or self.init_node):
            raise InitializationError("At least one of 'init_master' or 'init_node' must be True.")

        set_signal_handler(self.stop)

    def call(self, func_name):
        def dec(*args, **kwargs):
            if not self.node:
                raise GlobalContextError(
                    "The node has not been initialized in the current process."
                    " Make sure you passed 'init_node' = True in Global object."
                )
            msg = Message(
                flag=MessageFlag.REQUEST,
                transmitter=self.process_name,
                func_name=func_name,
                args=args,
                kwargs=kwargs
            )
            self.node.send(msg.dumps())
        return dec

    def start(self):
        self.worker_thread = Thread(target=self._start, daemon=True)
        self.worker_thread.start()
        # Wait initialization of Master and Node
        self.start_event.wait()

    def _start(self):

        if self.init_master:
            master_status = self.backend.read(BackEndKeys.MASTER_STATUS)
            master_hc_ts = self.backend.read(BackEndKeys.MASTER_HEALTHCHECK_TS)
            if (
                    master_status != MasterStatus.RUNNING
                    or
                    not master_hc_ts
                    or
                    (time.time() - master_hc_ts) > UnixSocketMaster.TIMEOUT
            ):
                self.backend.delete_key(BackEndKeys.MASTER_HEALTHCHECK_TS)
                self.backend.delete_key(BackEndKeys.MASTER_STATUS)
            try:
                self.backend.write_if_not_exist(BackEndKeys.MASTER_STATUS, MasterStatus.RUNNING)
                self.master = UnixSocketMaster(self.process_name, self.backend, self.stop_event)
            except ValueError:
                # Master has been initialized by other process
                ...

        if self.init_node:
            self.node = UnixSocketNode(self.process_name, self.backend, self.stop_event)

        if self.node or self.master:
            m_future = n_future = wait_future = None
            with start_blocking_portal() as portal:
                if self.master:
                    m_future = portal.start_task_soon(self.master.handle)
                    wait_future, _ = portal.start_task(self.master.wait)
                if self.node:
                    n_future = portal.start_task_soon(self.node.handle)

                self.start_event.set()
                self.stop_event.wait()
                for future in filter(None, (wait_future, m_future, n_future)):
                    try:
                        future.cancel()
                    except RuntimeError:
                        ...

    def stop(self, *args, **kwargs):
        self.stop_event.set()
        if self.master:
            self.backend.delete_key(BackEndKeys.MASTER_HEALTHCHECK_TS)
            self.backend.write(BackEndKeys.MASTER_STATUS, MasterStatus.UNAVAILABLE)
        self.worker_thread.join()
