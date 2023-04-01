from inspect import isclass
from dataclasses import dataclass
from typing import Optional, Union, List
from daffi.utils.custom_types import TimeUnits


__all__ = [
    "FG",
    "BG",
    "PERIOD",
    "BROADCAST",
    "ALL_EXEC_MODIFIERS",
    "is_exec_modifier",
    "is_exec_modifier_type",
]


@dataclass
class FG:
    timeout: Optional[TimeUnits] = None


@dataclass
class BG:
    timeout: Optional[TimeUnits] = None
    eta: Optional[TimeUnits] = None
    return_result: Optional[bool] = True


@dataclass
class PERIOD:
    at_time: Optional[Union[List[TimeUnits], TimeUnits]] = None
    interval: Optional[TimeUnits] = None


@dataclass
class BROADCAST:
    eta: Optional[TimeUnits] = None
    timeout: Optional[TimeUnits] = None  # Works only with return_result=True
    return_result: Optional[bool] = True


ALL_EXEC_MODIFIERS = (FG, BG, PERIOD, BROADCAST)


def is_exec_modifier(candidate: Union[object, type]) -> bool:
    """Check if provided candidate is instance of one of exec modifiers or class of one of exec modifiers"""
    if isclass(candidate):
        return issubclass(candidate, ALL_EXEC_MODIFIERS)
    return isinstance(candidate, ALL_EXEC_MODIFIERS)


def is_exec_modifier_type(candidate: Union[object, type], exec_modifier: Union["ALL_EXEC_MODIFIERS"]):
    if isclass(candidate):
        return issubclass(candidate, exec_modifier)
    return isinstance(candidate, exec_modifier)
