import os
import json
import traceback
import threading

import glog as log
from flask import Flask, jsonify

from vitamincv.apis.comm_api import CommAPI
from vitamincv.module import Module
from vitamincv.data.request import Request
from vitamincv.data.response import SchemaResponse


HEALTHPORT = os.environ.get("PORT", 5000)


class Server(Flask):
    def __init__(self, modules, input_comm, output_comms=None, use_schema_registry=True):
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
            raise TypeError("modules is not a list of Module")
        if len(modules) == 0:
            raise TypeError("No modules was provided.")
        for m in modules:
            if not isinstance(m, Module):
                raise TypeError("Not all the modules are of type Module")
        if not input_comm:
            raise TypeError("No input_comm set")
        if not output_comms:
            output_comms = [input_comm]
        if not isinstance(output_comms, list):
            output_comms = [output_comms]
        for out in output_comms:
            if not isinstance(out, CommAPI):
                raise TypeError("comm_apis_outputs must be CommAPIs")

        self.input_comm = input_comm
        self.output_comms = output_comms
        self.modules_info = [{"name": x.name, "version": x.version} for x in modules]
        self.modules = modules
        self.use_schema_registry = False

        log.info("Input comm type: {}".format(type(input_comm)))
        for out in output_comms:
            log.info("Output comm type(s): {}".format(type(out)))

        super().__init__(__name__)

        @self.route("/health", methods=["GET"])
        def health_check():
            return jsonify(self.modules_info)

    def start(self):
        """Entry point for starting a server.

            Note: this starts a healthcheck endpoint in a separate thread
        """
        log.info(f"Starting HealthCheck endpoint at /health on port {HEALTHPORT}")
        try:
            threading.Thread(
                target=self.run, kwargs={"host": "0.0.0.0", "port": HEALTHPORT}, daemon=True
            ).start()
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())
            log.error("Error setting up healthcheck endpoint")
        log.info("Starting server...")
        self._start()

    def _start(self):
        while True:
            try:
                log.info("Pulling request")
                requests = self.input_comm.pull()
                for request in requests:
                    schema_response = self._process_request(request)
                    log.info("Pushing reponse to output_comms")
                    for output_comm in self.output_comms:
                        try:
                            ret = output_comm.push(schema_response)
                        except Exception as e:
                            log.error(e)
                            log.error(traceback.format_exc())
                            log.error(f"Error pushing to output_comm: {output_comm}")
            except Exception as e:
                log.error(e)
                log.error(traceback.format_exc())
                log.error("Error processing request")

    def _process_request(self, request):
        """Send request_message through all the modules

        Args:
            request (Request): incoming request

        Returns:
            Response: outgoing response message
        """
        if not isinstance(request, Request):
            raise ValueError(f"request is of type {type(request)}, not Request")
        log.info(f"Processing: {request}")

        schema_response = SchemaResponse(request, self.use_schema_registry)
        response = schema_response.to_module_response()

        for module in self.modules:
            log.info(f"Processing request for module: {module}")
            response = module.process(response)
            log.debug(f"response.dict: {json.dumps(response.dict, indent=2)}")

        return SchemaResponse(response, self.use_schema_registry)
