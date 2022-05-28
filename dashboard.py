'''
Streamlit dashboard
'''
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import streamlit as st
from rq import Queue
from rq.job import Job

from predict_from_model import Prediction
# import background job functions
from prediction_validation_insertion import PredictionValidation
from worker import conn


# manager function
def manager(queue:Queue, func: Callable, args: tuple[Any] | None=None, timeout: int=300,
            max_retries:int=3, max_wait: int=400,
            status_msg: str='Processing request...',
            failure_msg: str='Failed to process request!',
            success_msg: str='Request Processed successfully!'):

    # enque the job
    queue.empty()
    job = queue.enqueue_call(func, args, timeout=timeout,
                        result_ttl=300, failure_ttl=120, ttl=900,)

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
            job = Job.fetch(job.id, connection=conn)
            st.error(job.exc_info)
            job.delete()
            raise RuntimeError(failure_msg)

        # unexpected error this should not happen
        if max_retries > 0:
            st.warning('Unexpected error occurred while processing request. Retrying')
            job.delete()
            return manager(q, func, args, timeout=timeout, max_retries=max_retries-1,
                            max_wait=max_wait, status_msg=status_msg,
                            failure_msg=failure_msg, success_msg=success_msg)

        raise RuntimeError(
                    'An unexpected error has occurred while processing request.')
    else:
        # job has finished successfully
        st.success(success_msg)
        job.delete()


# title
st.title('RUL predictor')

try:
    q = Queue(connection=conn)

    if st.button('Prediction with stock files'):
        # default mode
        st.info('Importing files from database')
        Importer = PredictionValidation()
        manager(q, func=Importer.pred_fetch, args=None, timeout=900, max_wait=1000,
                failure_msg='Importing files from database failed',
                success_msg='Imported files from database successfully!')

        st.info('Generating predictions from imported data')
        Predictor = Prediction('Prediction_FileFromDB')
        manager(q, func=Predictor.prediction_from_model, args=None, timeout=300, max_wait=400,
                failure_msg='Failed to generate predictions.',
                success_msg='Predictions saved successfully!')

except Exception as e:
    raise
