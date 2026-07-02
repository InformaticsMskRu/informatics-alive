import functools

do_once = functools.lru_cache(1)

@do_once
def make_submit_task_chain():
    """
    Returns chain for sending celery task for non terminal statuses,
    e.g. Compiling.
    Cache for only once module importing
    """
    from .task import (
        submit_task,
    )

    submit_task_chain = submit_task.s()
    return submit_task_chain