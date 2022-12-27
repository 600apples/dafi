import os
import shelve
from functools import cached_property
from pathlib import Path
from tempfile import gettempdir
from typing import Any, NoReturn, Optional

from filelock import FileLock

from dafi.interface import BackEndI


class BackEndKeys:
    CONTROLLER_STATUS = "controller_status"
    CONTROLLER_NAME = "controller_name"
    CONTROLLER_HEALTHCHECK_TS = "controller_healthcheck_ts"


class LocalBackEnd(BackEndI):
    IPC_STORAGE = Path(gettempdir()) / "dafi"

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
            try:
                return backend_store[key]
            except (AttributeError, KeyError):
                return default

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
