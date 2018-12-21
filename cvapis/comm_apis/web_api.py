import os
import glog as log
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from time import sleep

#from subprocess import Popen
#import shlex

#from rq import Queue, Worker, Connection
#from redis import Redis

from cvapis.comm_apis.comm_api import CommAPI

class WebAPI(CommAPI):
    def __init__(self, port=5000):
        super().__init__()
        self.port = port

    def pull(self):
        app = Flask(__name__)
        CORS(app)

        @app.route("/", methods=["GET", "POST"])
        def process():
            message = request.get_json(force=True)
            log.info('Message received: {}'.format(message))
            response = self.cvmodule.process(message)
            if isinstance(response, dict):
                return json.dumps(response, indent=2)
            return response
            # job = self.q.enqueue(self.cvmodule.process, message)
            # while not job.result:
                # sleep(0.1)
            # return jsonify(job.result)

        @app.route("/health", methods=["GET", "POST"])
        def health():
            return "Healthy"

        log.info("Ready to process message using {}".format(type(self.cvmodule)))
        log.info("Flask server is listening on port: {}".format(self.port))
        app.run(debug=True, port=self.port)
