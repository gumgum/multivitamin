import os
import traceback
import glog as log
import argparse
import threading
import socket

from flask import Flask, jsonify

from vitamincv.comm_apis.comm_api import CommAPI
from vitamincv.comm_apis.sqs_api import SQSAPI
from vitamincv.comm_apis.local_api import LocalAPI
from vitamincv.comm_apis.vertex_api import VertexAPI
from vitamincv.module_api.cvmodule import CVModule

from vitamincv.processor import Processor
from vitamincv.data.request_message import Request
from vitamincv.data.response_message import Response
from vitamincv.data.struct import CVData

PORT = os.environ.get('PORT', 5000)

class AsyncServer(Flask):
    def __init__(self, cvmodules, input_comm, output_comms=None):
        """AsyncServer serves as the public interface for CV services through VitaminCV

        It's role is to start the healthcheck endpoint and initiate the services

        Args:
            cvmodules (list[CVModule]): list of concrete child implementation of CVModule
            input_comm (CommAPI): Concrete child implementation of CommAPI, called for pulling 
                                  responses to process
            output_comms (list[CommAPI]): List of concrete child implementations of CommAPI, 
                                          called for pushing responses to somewhere
        """
        if isinstance(cvmodules, CVModule):
            cvmodules = [cvmodules]
            
        if not isinstance(cvmodules, list):
            raise TypeError("modules is not a list of CVModule")

        if len(cvmodules)==0:
            raise TypeError("No modules was provided.")

        for m in cvmodules:
            if not isinstance(m, CVModule):
                raise TypeError("Not all the modules are of type CVModule")

        if not input_comm:
            raise TypeError("No input_comm set")

        if not output_comms:
            output_comms = [input_comm]

        if not isinstance(output_comms, list):
            output_comms = [output_comms]

        for out in output_comms:
            if not isinstance(out, CommAPI):
                raise TypeError("comm_apis_outputs must be CommAPIs")
            
        self._controller = Controller(cvmodules)
        self._input_comm = input_comm
        self._output_comms = output_comms
        log.info("Input comm type: {}".format(type(input_comm)))
        for out in output_comms:
            log.info("Output comm type(s): {}".format(type(out)))
        
        self.modules_info = [{"name": x.name, "version": x.version} for x in cvmodules]
        super().__init__(__name__)
        
        @self.route("/health", methods=["GET"])
        def health_check():
            return jsonify(self.modules_info)

    def start(self):
        """Entry point for starting a server.

            Note: this starts a healthcheck endpoint in a separate thread
        """
        log.info("Starting HealthCheck endpoint at /health on port {}".format(PORT))
        threading.Thread(target=self.run, kwargs={"host":"0.0.0.0","port": PORT}, daemon=True).start()
        log.info("Starting CVmodule server")
        self._start()

    def _start(self):
        while True:
            try:
                log.info("Pulling request")
                request = self.input_comm.pull()
                response = self._controller.process_request(request)
                log.info("Pushing reponse to output_comms")
                for output_comm in self.output_comms:
                    try:
                        ret = output_comm.push(response)
                    except Exception as e:
                        log.info(e)
                        log.info(traceback.format_exc())
            except Exception as e:
                log.info(e)
                log.info(traceback.format_exc())
                
