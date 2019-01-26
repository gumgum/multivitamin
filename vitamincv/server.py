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

class Server(Flask):
    def __init__(self, cvmodules, input_comm, output_comms=None):
        """Constructor requires 

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
            
        self._controller = Controller()
        self._cvmodules = cvmodules
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
                response = self._process_request(request)
                log.info("Pushing reponse to output_comms")
                for c in self.output_comms:
                    try:
                        ret = c.push(response)
                    except Exception as e:
                        log.info(e)
                        log.info(traceback.format_exc())
            except Exception as e:
                log.info(e)
                log.info(traceback.format_exc())
                
    def _process_request(self, request_message):
        """Send request_message through all the cvmodules

        Args:
            request_message (Request): incoming request
            previous_response_message (Response): previous response message
        Returns:
            Response: outgoing response message
        """"
        if not isinstance(request_message, Request):
            raise ValueError(f"request_message is of type {type(request_message)}, not Request")
        
        response_message = None
        if request_message.get_prev_response():
            log.info("Request contains previous response")
            response_message = Response(
                    bin_decoding=request_message.bin_decoding(), 
                    prev_response=request_message.get_prev_response()
                )

        if request_message.get_prev_response_url():
            if response_message is not None:
                raise ValueError(f"request_message contains both prev_response and prev_response_url")
            log.info("Request contains previous response url")
            response_message = Response(
                    bin_decoding=request_message.bin_decoding(), 
                    prev_response_url=request_message.get_prev_response_url()
                )

        for module in self._cvmodules:
            log.info(f"Processing request for cvmodule: {type(module)}")
            response_message = Mediator.convert_cvdata_to_response(module, request_message, response_message)

        if request_message.bin_encoding():
            return response_message.serialize()
        return response_message.dict()

from abc import abstractmethod, ABC

class DataMediator(ABC):
    """DataMediator has 1 job: to convert responses to CVData, and CVdata back to responses"""
    @abstractmethod
    def convert_response_to_cvdata(response_message):
        pass
    
    @abstractmethod
    def convert_cvdata_to_response(cvdata):
        pass

    @abstractmethod
    def process(request_message, response_message):
        pass
        
class ImageDataMediator(DataMediator):
    def convert_response_to_cvdata(response_message):
        print("hi")

    def convert_cvdata_to_response(cvdata):
        print("ho")

class PropertiesDataMediator(DataMediator):
    def convert_response_to_cvdata(response_message):
        print("hi")

    def convert_cvdata_to_response(cvdata):
        print("ho")

        cvdata = None

        if response_message:
            if isinstance(cvmodule, PropertiesProcessor):
                cvdata = getsegments(cvdata)
                segments = cvmodule.process(request_message, segments)
            elif isinstance(cvmodule, ImageProcessor):
                #get detections
                detections = cvmodule.process(request_message, detections)
            else:
                raise ValueError("not a prop proc or image proc")

        return response_message