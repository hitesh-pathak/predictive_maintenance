from flask import Flask, render_template, request
from rq import Queue
from rq.job import Job
from worker import conn
from gen_prediction import gen_prediction


# Initialise Flask app
app = Flask(__name__)

# set up a redis queue based on the connection
q = Queue(connection=conn, default_timeout = 1000)

# Set up main route
@app.route('/', methods=['GET', 'POST'])
def index():
    results = {}
    if request.method == 'POST':
        # Extract the input
        path = request.form.get('filepath')

        # put the prediction job into redis queue
        job = q.enqueue_call(func=gen_prediction, args=(path, ), result_ttl=1000,)

        print(job.get_id())

        return render_template("index.html", results=results)

    elif request.method == 'GET':
        return render_template('index.html')

@app.route('/results/<job_key>', methods=['GET'])
def get_results(job_key):

    job = Job.fetch(job_key, connection=conn)

    if job.is_finished:
        return str(job.result), 200

    else:
        return 'Not finished yet', 202
