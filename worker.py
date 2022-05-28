'''
Bacground worker that uses Redis queue to process requests.
'''
import os
import sys
import redis
from rq import Worker, Queue, Connection

listen = ['default']

redis_url = os.getenv('REDISTOGO_URL',
            'redis://:simpulpass.@redis-13792.c301.ap-south-1-1.ec2.cloud.redislabs.com:13792')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    try:
        with Connection(conn):
            worker = Worker(list(map(Queue, listen)))
            worker.work()

    except KeyboardInterrupt as interrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
        