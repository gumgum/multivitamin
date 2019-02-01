import os
import traceback
import glog as log
import argparse
import threading
import socket

from flask import Flask, jsonify

from vitamincv.apis.comm_api import CommAPI
from vitamincv.apis.sqs_api import SQSAPI
from vitamincv.apis.local_api import LocalAPI
from vitamincv.apis.vertex_api import VertexAPI
from vitamincv.module import Module

from vitamincv.controller import Controller
from vitamincv.data.request import Request
from vitamincv.data.response_interface import Response
from vitamincv.data import MediaData

PORT = os.environ.get("PORT", 5000)


class Server(Flask):
    def __init__(self, modules, input_comm, output_comms=None):
        """Serves as the public interface for CV services through VitaminCV

        It's role is to start the healthcheck endpoint and initiate the services

        Args:
            modules (list[Module]): list of concrete child implementation of CVModule
            input_comm (CommAPI): Concrete child implementation of CommAPI, called for pulling 
                                  responses to process
            output_comms (list[CommAPI]): List of concrete child implementations of CommAPI, 
                                          called for pushing responses to somewhere
        """
        if isinstance(modules, Module):
            modules = [modules]

        if not isinstance(modules, list):
            raise TypeError("modules is not a list of CVModule")

        if len(modules) == 0:
            raise TypeError("No modules was provided.")

        for m in modules:
            if not isinstance(m, Module):
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

        self.controller = Controller(modules)
        self.input_comm = input_comm
        self.output_comms = output_comms
        log.info("Input comm type: {}".format(type(input_comm)))
        for out in output_comms:
            log.info("Output comm type(s): {}".format(type(out)))

        super().__init__(__name__)

        @self.route("/health", methods=["GET"])
        def health_check():
            return jsonify(self._controller.modules_info)

    def start(self):
        """Entry point for starting a server.

            Note: this starts a healthcheck endpoint in a separate thread
        """
        log.info(f"Starting HealthCheck endpoint at /health on port {PORT}")
        threading.Thread(target=self.run, kwargs={"host": "0.0.0.0", "port": PORT}, daemon=True).start()
        log.info("Starting server...")
        self._start()

    def _start(self):
        while True:
            try:
                log.info("Pulling request")
                requests = self.input_comm.pull()
                for request in requests:
                    response = self.controller.process_request(request)
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
