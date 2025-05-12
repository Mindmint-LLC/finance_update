
import os
import subprocess
import threading
import queue
from flask import Flask, render_template, Response
from _tools import login_required
import _google_oauth


app = Flask(__name__)
app.secret_key = os.urandom(42)
app.register_blueprint(_google_oauth.gauth_bp)

output_queue = queue.Queue()
process = None
AUTHORIZED_USERS = os.getenv('AUTHORIZED_USERS')


def run_process(command):
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        bufsize=1,
        universal_newlines=True
    )
    def enqueue_output(pipe, prefix=''):
        for line in iter(pipe.readline, ''):
            output_queue.put(line.strip())
        pipe.close()
    stdout_thread = threading.Thread(target=enqueue_output, args=(process.stdout,), daemon=True)
    stderr_thread = threading.Thread(target=enqueue_output, args=(process.stderr, 'ERROR: '), daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    return process


def stream_output():
    while True:
        message = output_queue.get()
        yield f"data: {message}\n\n"


@app.route('/')
@login_required(authorized_users=AUTHORIZED_USERS)
def home():
    return render_template('index.html')


@app.route('/log_stream')
@login_required(authorized_users=AUTHORIZED_USERS)
def log_stream():
    run_process("bash run.sh")
    return render_template('log_stream.html')


@app.route('/stream')
def stream_logs():
    return Response(
        stream_output(),
        mimetype='text/event-stream'
    )


if __name__ == '__main__':
    app.run(debug=True, port=8080)
