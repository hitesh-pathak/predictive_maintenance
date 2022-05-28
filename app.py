'''
FastAPI backend
'''
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from rq import Queue
from rq.job import Job
from worker import conn
from gen_prediction import gen_prediction
from custom_modules import FilePath, BackgroundTask, Result


app = FastAPI()

# set up redis queue
q = Queue(connection=conn, default_timeout=1000)

@app.post('/default', status_code=status.HTTP_202_ACCEPTED, response_model=BackgroundTask)
async def start_prediction(filepath: FilePath):
    '''
        Create a prediction job and return the job id.
    '''

    job = q.enqueue_call(func=gen_prediction, args=(filepath.filepath, ), result_ttl=1000)
    
    return {'job_id': str(job.get_id()), 'status': 'Processing'}


@app.post('/fetch', status_code=status.HTTP_202_ACCEPTED, response_model=BackgroundTask)
async def start_fetching():
    pass
    # !!! abort this trying something else


@app.get('/results/{job_id}', status_code=status.HTTP_200_OK, response_model=Result,
        responses={status.HTTP_202_ACCEPTED:
         {'model': BackgroundTask, 'description': 'Processing in the background'}},)
async def get_result(job_id: str):
    '''
        Fetch status for a given background job by its id.
    '''
    job = Job.fetch(job_id, connection=conn)

    if not job.is_finished:
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED,
                             content={'job_id': job_id, 'status': 'Processing...'})

    result = job.result
    return {'result': result}
