from flask import Flask, render_template, request
from rq import Queue
import json
from rq.job import Job
from worker import conn
from gen_prediction import gen_prediction


# Initialise Flask app
app = Flask(__name__)

# set up a redis queue based on the connection
q = Queue(connection=conn, default_timeout = 1000)

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@app.route('/start', methods=['POST'])
def get_counts():

    # get url
    data = json.loads(request.data.decode())
    filepath = data["filepath"]

    # start job
    job = q.enqueue_call(
        func=gen_prediction, args=(filepath,), result_ttl=1000
    )
    # return created job id
    return job.get_id()
