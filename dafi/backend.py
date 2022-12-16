import os
import shelve
from functools import cached_property
from pathlib import Path
from tempfile import gettempdir
from typing import Any, NoReturn, Optional

from filelock import FileLock
from dafi.interface import BackEnd


class BackEndKeys:
    MASTER_STATUS = "master_status"
    MASTER_NAME = "master_name"
    MASTER_HEALTHCHECK_TS = "master_healthcheck_ts"


class LocalBackEnd(BackEnd):
    IPC_STORAGE = Path(gettempdir()) / "ipc"

    @property
    def store(self):
        return shelve.open((self.base_dir / ".store").as_posix())

    @cached_property
    def lock(self):
        fl = FileLock(self.base_dir / ".lock")
        fl.release(force=True)
        fl.timeout = 10
        return fl

    @cached_property
    def base_dir(self) -> os.PathLike:
        self.IPC_STORAGE.mkdir(parents=True, exist_ok=True)
        return self.IPC_STORAGE

    def read(self, key: str, default: Optional[Any] = None) -> Any:
        with self.store as backend_store:
            return backend_store.get(key, default)

    def write(self, key: str, value: Any) -> NoReturn:
        with self.lock, self.store as backend_store:
            backend_store[key] = value

    def write_if_not_exist(self, key: str, value: Any) -> NoReturn:
        with self.lock, self.store as backend_store:
            if backend_store.get(key):
                raise ValueError(f"Key {key!r} already exists.")
            backend_store[key] = value

    def delete_key(self, key: str):
        with self.lock, self.store as backend_store:
            try:
                del backend_store[key]
            except KeyError:
                ...

