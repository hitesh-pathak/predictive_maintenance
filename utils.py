"""
This module contains various utility functions
"""
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import streamlit as st
from rq import Queue
from rq.job import Job


# manager function
def manager(queue: Queue, connection,
            func: Callable, args: tuple[Any] | None = None, timeout: int = 300,
            max_retries: int = 3, max_wait: int = 400,
            status_msg: str = 'Processing request...',
            failure_msg: str = 'Failed to process request!',
            success_msg: str = 'Request Processed successfully!'):
    """
    This function handles job queueing into the redis queue.

    :param queue: Redis queue object
    :param connection: Redis connection object
    :param func: Callable to enqueue
    :param args: arguments passed to callable
    :param timeout: maximum time after which job is declared to be failed
    :param max_retries: maximum times to retry after a job fails to queue properly
    :param max_wait: maximum time to wait for a job result
    :param status_msg: message to print while the job is processing
    :param failure_msg: message to print if the job is failed
    :param success_msg: message to show if job is successful
    :return: None
    """

    # enqueue the job
    queue.empty()
    job = queue.enqueue_call(func, args, timeout=timeout,
                             result_ttl=300, failure_ttl=120, ttl=900, )

    # check status in case the status is immediate
    status = job.get_status()
    start = datetime.now()

    with st.spinner(status_msg):
        while True:
            if status in ['queued', 'started']:
                status = job.get_status()
                now = datetime.now()
                if now - start > timedelta(seconds=max_wait):
                    break
                continue
            break

    if not job.is_finished:
        if job.is_failed:
            # show traceback
            job = Job.fetch(job.id, connection=connection)
            st.error(job.exc_info)
            job.delete()
            raise RuntimeError(failure_msg)

        # unexpected error this should not happen
        if max_retries > 0:
            st.warning('Unexpected error occurred while processing request. Retrying')
            job.delete()
            return manager(queue, func, connection, args, timeout=timeout,
                           max_retries=max_retries - 1,
                           max_wait=max_wait, status_msg=status_msg,
                           failure_msg=failure_msg, success_msg=success_msg)

        raise RuntimeError(
            'An unexpected error has occurred while processing request.')
    else:
        # job has finished successfully
        st.success(success_msg)
        job.delete()
