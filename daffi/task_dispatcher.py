import json
import time
import sys
import dfcore
from queue import Queue, Empty
from threading import Thread, Event
from daffi.registry.executor import EXECUTOR_REGISTRY
from daffi.serialization import Serializer, SerdeFormat
from daffi.core import send_message_from_client, send_message_from_service, MessageFlag
from daffi.misc import calc_sleep_and_backoff

import tblib.pickling_support

tblib.pickling_support.install()


class TaskDispatcher:

    def __init__(self):
        self.connections = set()
        self.stop_event = Event()
        self.pollers = (dfcore.getMessageForClientWorker, dfcore.getMessageForServerWorker)
        self.threads = []
        self.queue = Queue()

    def stop_for_connection(self, connection):
        self.connections.discard(connection)
        if not self.connections:
            self.stop_event.set()
            for thread in self.threads:
                thread.join()

    def start_for_connection(self, connection):
        assert connection._conn_num is not None
        self.connections.add(connection)
        if not self.threads:
            if not self.threads:
                for _ in range(2):
                    worker = TaskWorker(self.queue, self.stop_event)
                    worker_thread = Thread(target=worker.handle_tasks)
                    worker_thread.start()
                    self.threads.append(worker_thread)
                self_thread = Thread(target=self.handle_task_queue, daemon=True)
                self_thread.start()
                self.threads.append(self_thread)

    def handle_task_queue(self):
        initial_sleep, burst_sleep = .2, .0001
        sleep, backoff = initial_sleep, 1000
        sleep_anb_backoff = calc_sleep_and_backoff(0.0001, initial_sleep)
        while not self.stop_event.is_set():
            for task, conn in (
                    (res, conn)
                    for conn in self.connections if (res := self.pollers[bool(conn.server_mode)](conn._conn_num))
            ):
                # process task
                sleep = burst_sleep
                uuid, data, flag, serde, transmitter, receiver, func_name, return_result = task
                self.queue.put((uuid, data, flag, serde, transmitter, func_name, return_result, conn))
            sleep, backoff = sleep_anb_backoff(sleep, backoff)
            time.sleep(sleep)


class TaskWorker:

    def __init__(self, queue: Queue, stop_event: Event):
        self.queue = queue
        self.stop_event = stop_event
        self.senders = (send_message_from_client, send_message_from_service)

    def handle_tasks(self):
        while not self.stop_event.is_set():
            try:
                uuid, data, flag, serde, transmitter, func_name, return_result, conn = self.queue.get(timeout=5)
                if flag == MessageFlag.EVENTS:
                    data = json.loads(data)
                    for handler in conn.event_handlers:
                        handler(data)
                    continue

                cb = EXECUTOR_REGISTRY.get(func_name)
                args, kwargs = Serializer.deserialize(serde, data)
                try:
                    flag = MessageFlag.RESPONSE
                    result = cb(*args, **kwargs)
                except Exception:
                    flag = MessageFlag.ERROR
                    if serde == SerdeFormat.RAW:
                        serde = SerdeFormat.PICKLE
                    err_type, err_obj, traceback = sys.exc_info()
                    if serde == SerdeFormat.PICKLE:
                        result = (err_type.__name__, err_type.__module__, str(err_obj), traceback)
                    else:
                        result = (err_type.__name__, err_type.__module__, str(err_obj), None)
                if return_result:
                    result, is_bytes = Serializer.serialize(serde, result)
                    self.senders[bool(conn.server_mode)](
                        data=result,
                        flag=flag,
                        serde=serde,
                        receiver=transmitter,
                        func_name=func_name,
                        return_result=False,
                        conn_num=conn._conn_num,
                        is_bytes=is_bytes,
                        uuid=uuid
                    )
            except Empty:
                continue
