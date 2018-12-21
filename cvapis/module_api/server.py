import os
import traceback
import glog as log
import argparse
import threading
import socket

from flask import Flask, jsonify

from cvapis.comm_apis.comm_api import CommAPI
from cvapis.comm_apis.sqs_api import SQSAPI
from cvapis.comm_apis.local_api import LocalAPI
from cvapis.comm_apis.vertex_api import VertexAPI
from cvapis.module_api.cvmodule import CVModule

PORT = os.environ.get('PORT', 5000)

class Server(Flask):
    def __init__(self, modules, input_comm, output_comms=None):
        """Constructor requires a communication api and a module api.

        Args:
            modules (list[CVModule]): list of concrete child implementation of CVModule
            input_comm (CommAPI): Concrete child implementation of CommAPI, called for pulling 
                                  responses to process
            output_comms (list[CommAPI]): List of concrete child implementations of CommAPI, 
                                          called for pushing responses to somewhere
        """
        if isinstance(modules, CVModule):
            modules=[modules]
            
        if not isinstance(modules, list):
            raise TypeError("modules is not a list of CVModule")
        else:
            if len(modules)==0:
                raise TypeError("No modules was provided.")
            for m in modules:
                if not isinstance(m, CVModule):
                    raise TypeError("Not all the modules are of type CVModule")
        if not output_comms:
            output_comms = [input_comm]
        if not isinstance(output_comms, list):
            output_comms = [output_comms]

        for out in output_comms:
            if not isinstance(out, CommAPI):
                raise TypeError("comm_apis_outputs must be CommAPIs")
            
        self.modules = modules
        self.input_comm = input_comm
        self.output_comms = output_comms
        self.stop_flag = False
        self.pull_batch_size = 1
        log.info("Input comm type: {}".format(type(input_comm)))
        for out in output_comms:
            log.info("Output comm type(s): {}".format(type(out)))
        
        self.modules_info = [{"name": x.name, "version": x.version} for x in modules]
        super().__init__(__name__)
        
        @self.route("/health", methods=["GET"])
        def health_check():
            return jsonify(self.modules_info)

    def _start(self):
        while self.stop_flag == False:
            try:
                log.info("Pulling request")
                request = self.input_comm.pull(self.pull_batch_size)  # This returns a vector of Request_API                
                for i,m in enumerate(self.modules):
                    log.info('***************')
                    log.info("Processing request with module " + m.name)
                    codes = m.process(request)
                    log.info("Updating response")
                    m.update_response()
                    request=m.get_request_api()
                    if i<len(self.modules)-1:
                        log.info("Reseting media_api in the requests.")
                        if isinstance(request,list):
                            for r in request:
                                r.reset_media_api()
                        else:
                            request.reset_media_api()
                log.info('***************')
                log.info("Pushing reponses")                
                for c in self.output_comms:
                    try:
                        ret = c.push(request)
                    except Exception as e:
                        log.info(e)
                        log.info(traceback.format_exc())

                log.info("*************************")
                log.info("End of iteration in while loop.")
            except Exception as e:
                log.info(e)
                log.info(traceback.format_exc())

    def start(self):
        """Entry point for starting a server.

            Note: this starts a healthcheck endpoint in a separate thread
        """
        log.info("Starting HealthCheck endpoint at /health on port {}".format(PORT))
        threading.Thread(target=self.run, kwargs={"host":"0.0.0.0","port": PORT}, daemon=True).start()
        log.info("Starting CVmodule server")
        self._start()

def run_server(cvmodule):
    """Entry point to run common server configurations

    Args:
        cvmodule (CVModule): Concrete child implementation of CVModule
    """
    a = argparse.ArgumentParser("Entry point for common server configurations")
    a.add_argument("--input_comm", required=True, choices=["sqs","local", "vertex"], help="Input communcation API")
    a.add_argument("--output_comm", required=True, choices=["local", "http"], help="Output communcation API")
    a.add_argument("--queue_name")
    a.add_argument("--local_in", help="local drive folder for input jsons")
    a.add_argument("--local_out", help="local drive to write output jsons")
    args = a.parse_args()

    in_comm_api = None
    out_comm_api = None
    
    if args.input_comm == "sqs":
        if not args.queue_name:
            raise ValueError("args.queue_name required for args.input_comm == 'sqs'")
        in_comm_api = SQSAPI(args.queue_name)

    if args.input_comm == "local":
        if not args.local_in:
            raise ValueError("args.local_in required for args.input_comm == 'local'")
        in_comm_api = LocalAPI(pulling_folder=args.local_in)

    if args.output_comm == "local":
        if not args.local_out:
            raise ValueError("args.local_out required for args.output_comm == 'local'")
        out_comm_api = LocalAPI(pushing_folder=args.local_out)

    if args.output_comm == "http":
        raise NotImplementedError("Not yet implemented")

    if args.input_comm == "vertex":
        if not args.queue_name:
            raise ValueError("args.queue_name required for args.input_comm == 'vertex'")
        in_comm_api = VertexAPI(args.queue_name)
        out_comm_api = None

    if not in_comm_api:
        raise ValueError("Please set in_comm_api")

    server = Server(cvmodule, in_comm_api, out_comm_api)
    server.start()
