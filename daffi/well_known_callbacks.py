"""
Internal callbacks are utilized to facilitate the exchange of metadata information between processes.
"""
from typing import (
    Callable,
    Any,
    Optional,
    List,
    Dict,
)
from tenacity import retry, retry_if_exception_type, wait_fixed, stop_after_attempt
from daffi.decorators import callback
from daffi.exceptions import GlobalContextError


@callback
def __transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return func(*args, **kwargs)


@callback
async def __async_transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return await func(*args, **kwargs)


@callback
@retry(
    wait=wait_fixed(0.1),
    stop=stop_after_attempt(7),
    retry=retry_if_exception_type(
        RuntimeError,
    ),
    reraise=True,
)
async def __cancel_scheduled_task(msg_uuid: str, process_name: str, func_name: Optional[str] = None) -> bool:
    from daffi.components.scheduler import (
        logger,
        SCHEDULER_PERIODICAL_TASKS,
        SCHEDULER_AT_TIME_TASKS,
        FINISHED_TASKS,
        TaskIdent,
    )

    task_found = False
    if msg_uuid and func_name and process_name:
        task_ident = TaskIdent(process_name, func_name, msg_uuid)
        period_task = SCHEDULER_PERIODICAL_TASKS.pop(task_ident, None)
        if period_task:
            period_task.cancel()
            FINISHED_TASKS.append(task_ident.msg_uuid)
            logger.warning(f"Task {func_name!r} (condition=period, executor={process_name}) has been canceled.")
            task_found = True
        else:
            at_time_tasks = SCHEDULER_AT_TIME_TASKS.pop(task_ident, None)
            if at_time_tasks:
                for at_time_task in at_time_tasks:
                    if at_time_task is not None:
                        at_time_task.cancel()
                        FINISHED_TASKS.append(task_ident.msg_uuid)
                logger.warning(
                    f"Task group {func_name!r} (condition=period, executor={process_name}) has been canceled."
                )
                task_found = True

    elif msg_uuid:
        for task_ident, period_task in SCHEDULER_PERIODICAL_TASKS.items():
            if task_ident.msg_uuid == msg_uuid:
                period_task.cancel()
                SCHEDULER_PERIODICAL_TASKS.pop(task_ident, None)
                FINISHED_TASKS.append(msg_uuid)
                logger.warning(
                    f"Task {task_ident.func_name!r} (condition=period, executor={task_ident.process_name}) has been canceled."
                )
                task_found = True
                break
        else:
            for task_ident, at_time_tasks in SCHEDULER_AT_TIME_TASKS.items():
                if task_ident.msg_uuid == msg_uuid and at_time_tasks:
                    for at_time_task in at_time_tasks:
                        if at_time_task is not None:
                            at_time_task.cancel()
                    SCHEDULER_AT_TIME_TASKS.pop(task_ident, None)
                    FINISHED_TASKS.append(msg_uuid)
                    logger.warning(
                        f"Task group {task_ident.func_name!r} (condition=period, executor={task_ident.process_name}) has been canceled."
                    )
                    task_found = True
                    break
    if not task_found:
        if msg_uuid in FINISHED_TASKS:
            return False
        GlobalContextError(f"Unable to find task by uuid: {msg_uuid}").fire()
    return True


@callback
@retry(
    wait=wait_fixed(0.1),
    stop=stop_after_attempt(7),
    retry=retry_if_exception_type(
        RuntimeError,
    ),
    reraise=True,
)
async def __get_all_period_tasks(process_name: str) -> List[Dict]:
    from daffi.components.scheduler import SCHEDULER_PERIODICAL_TASKS, SCHEDULER_AT_TIME_TASKS

    res = []
    for task_ident in SCHEDULER_PERIODICAL_TASKS:
        if task_ident.process_name == process_name:
            res.append(
                {
                    "condition": "period",
                    "func_name": task_ident.func_name,
                    "uuid": task_ident.msg_uuid,
                }
            )

    for task_ident, at_time_tasks in SCHEDULER_AT_TIME_TASKS.items():
        if task_ident.process_name == process_name:
            res.append(
                {
                    "condition": "at_time",
                    "func_name": task_ident.func_name,
                    "uuid": task_ident.msg_uuid,
                    "number_of_active_tasks": len(list(filter(None, at_time_tasks))),
                }
            )
    return res
