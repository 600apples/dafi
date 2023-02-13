from collections import defaultdict
from typing import TypeVar, Mapping, Tuple, Any, DefaultDict, List, Dict

from daffi.store import TttStore


MsgUUID = TypeVar("MsgUUID", bound=int)
MsgResult = TypeVar("MsgResult", bound=Any)
Receiver = TypeVar("Receiver", bound=str)
Transmitter = TypeVar("Transmitter", bound=str)


class OneToOneCallStore(TttStore, Mapping[MsgUUID, Tuple[Transmitter, Receiver]]):
    """OneToOneCallStore represents communication between Transmitter and Receiver."""

    def on_delete(self, proc: str) -> DefaultDict[Transmitter, List[MsgUUID]]:
        # Find all messages that proc, scheduled for deletion is awaiting
        awaited_by_del_process = [uuid for uuid, (trans, rec) in self.items() if trans == proc]

        # Delete all messages that scheduled for deletion process is awaiting.
        for uuid in awaited_by_del_process:
            self.pop(uuid, None)

        # Find all transmitters that sent request and currently
        # wait for result from process that are scheduled for deletion
        awaited_transmitters = defaultdict(list)
        [awaited_transmitters[trans].append(uuid) for uuid, (trans, rec) in self.items() if rec == proc]
        return awaited_transmitters


class OneToManyCallStore(TttStore, Mapping[MsgUUID, Tuple[Transmitter, Dict[Receiver, MsgResult]]]):
    """OneToManyCallStore represents communication between one Transmitter and many Receivers (broadcast or stream)"""

    def on_delete(self, proc: str) -> DefaultDict[Transmitter, List[MsgUUID]]:
        # Find all messages that proc, scheduled for deletion is awaiting
        awaited_by_del_process = [uuid for uuid, (trans, _) in self.items() if trans == proc]

        # Delete all messages that scheduled for deletion process is awaiting.
        for uuid in awaited_by_del_process:
            self.pop(uuid, None)

        # Find all transmitters that sent request and currently
        # wait for result from process that are scheduled for deletion
        awaited_transmitters = defaultdict(list)
        [awaited_transmitters[trans].append(uuid) for uuid, (trans, agg_msg) in self.items() if proc in agg_msg]
        return awaited_transmitters
