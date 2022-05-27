'''
This module defines Pydantic classes used by FastAPI
'''
from pydantic import BaseModel


# pydantic models for fast api

class FilePath(BaseModel):
    '''
        Filepath for RUL prediction.
    '''

    filepath: str


class BackgroundTask(BaseModel):
    '''
    Provides information about background task.
    '''

    job_id: str
    status: str


class Result(BaseModel):
    '''
    The class holding the result string.
    '''

    result: str
