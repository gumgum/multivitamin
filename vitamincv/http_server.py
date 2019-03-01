import os
import json
import traceback

import glog as log
from flask import Flask, request, jsonify

from vitamincv.module import Module
from vitamincv.data.request import Request
from vitamincv.data.response import SchemaResponse


class HTTPServer(Flask):
    def __init__(self, modules, port=8888):
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
        self.modules_info = [{"name": x.name, "version": x.version} for x in modules]
        self.modules = modules
        self.port = port

        super().__init__(__name__)

        @self.route("/process", methods=["GET", "POST"])
        def process():
            """Entry point for starting an HTTP server
            """
            log.info("Pulling request")
            message = request.get_json(force=True)
            req = Request(request_dict=message)
            schema_response = SchemaResponse(request=req)
            response = schema_response.response

            for module in self.modules:
                log.info(f"Processing request for module: {module}")
                response = module.process(response)
                log.debug(f"response.dict: {json.dumps(response.dict, indent=2)}")

            sr = SchemaResponse(response=response)
            if req.bin_encoding:
                return sr.bytes
            else:
                return jsonify(sr.dict)

        @self.route("/health", methods=["GET"])
        def health_check():
            return jsonify(self.modules_info)

    def start(self):
        self.run(host="0.0.0.0", port=self.port)

