import os
import json
import traceback
import threading
import time

import glog as log
from flask import Flask, jsonify

from multivitamin.apis import CommAPI
from multivitamin.module import Module
from multivitamin.data import Response, Request


HEALTHPORT = os.environ.get("PORT", 5000)


class Server(Flask):
    def __init__(
        self, 
        modules,
        input_comm,
        output_comms=None,
        schema_registry_url=None,
    ):
        """Serves as the public interface for CV services through multivitamin

        It's role is to start the healthcheck endpoint and initiate the services

        Args:
            modules (list[Module]): list of concrete child implementation of Module
            input_comm (CommAPI): Concrete child implementation of CommAPI, called for pulling 
                                  responses to process
            output_comms (list[CommAPI]): List of concrete child implementations of CommAPI, 
                                          called for pushing responses to somewhere
            schema_registry_url (str): use schema in registry url instead of local schema
        """
        if isinstance(modules, Module):
            modules = [modules]
        assert(isinstance(modules, list))
        if len(modules) == 0:
            raise ValueError("No modules was provided.")
        for m in modules:
            assert(isinstance(m, Module))
        if not input_comm:
            raise TypeError("No input_comm set")
        if not output_comms:
            output_comms = [input_comm]
        if not isinstance(output_comms, list):
            output_comms = [output_comms]
        for out in output_comms:
            assert(isinstance(out, CommAPI))

        self.input_comm = input_comm
        self.output_comms = output_comms
        self.modules_info = [{"name": x.name, "version": x.version} for x in modules]
        self.modules = modules
        self.schema_registry_url = schema_registry_url

        log.info("Input comm type: {}".format(type(input_comm)))
        for out in output_comms:
            log.info("Output comm type(s): {}".format(type(out)))

        super().__init__(__name__)

        @self.route("/health", methods=["GET"])
        def health_check():
            return jsonify(self.modules_info)

    def start(self):
        """Public entry point for starting a server.

        Starts a healthcheck endpoint in a separate thread and calls self._start() which
        runs the actual server, pulling, processing, and posting requests
        """
        log.info(f"Starting HealthCheck endpoint at /health on port {HEALTHPORT}")
        try:
            threading.Thread(
                target=self.run,
                kwargs={"host": "0.0.0.0", "port": HEALTHPORT},
                daemon=True,
            ).start()
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())
            log.error("Error setting up healthcheck endpoint")
        log.info("Starting server...")
        self._start()

    end_flag=False
    def _start(self):
        """Start server. While loop that pulls requests from the input_comm, calls
        _process_request(request), and posts responses to output_comms
        """   
        requests=[]     
        while True:
            try:
                log.info("Pulling requests")
                #we get the number of requests we have to pull
                module0 =self.modules[0]
                n=module0.get_required_number_requests()
                requests = self.input_comm.pull(n)

                responses, self.end_flag = self._process_requests(requests)                
                for response in responses:
                    log.info("Pushing response to output_comms")
                    try:
                        ret = response._push(self.output_comms)
                    except Exception as e:
                        log.error(e)
                        log.error(traceback.format_exc())
                        log.error("Error pushing response to output_comms")               

                if self.end_flag:
                    log.info("end_flag activated")
                    #log.info("end_flag activated: Sleeping got 5 secs")
                    #time.sleep(5)
                    if len(module0.responses_to_be_processed)==0:
                        log.info("Finishing execution. end_flag activated.")
                        for r in module0.responses:
                            log.info(str(r.get_lifetime()) + ', module0.responses, potentially problematic url: ' + r.url)
                        break
                    else:
                        log.info("Trying to end but len(module0.responses_to_be_processed) = "+ str(len(module0.responses_to_be_processed)))
                        for r in module0.responses_to_be_processed:
                            log.info(str(r.get_lifetime()) + ', module0.responses_to_be_processed, potentially problematic url: ' + r.url)
            except Exception:
                log.error(traceback.format_exc())
                log.error(f"Error processing requests: {requests}")
            

    def _process_requests(self, requests):
        """Send request_message through all the modules

        Args:
            request (Request): incoming request

        Returns:
            Response: outgoing response message
        """
        end_flag=False  
        if not isinstance(requests, list):
            raise ValueError(f"request is of type {type(requests)}, not list")
        responses=[]
        for request in requests:
            log.info(f"Processing: {request}")
            if request.kill_flag is True:
                log.info(
                    "Incoming request with kill_flag == True, killing server"
                )
                end_flag=True 
                break                            
            log.info(f"Processing url: {request.get('url')}")
            response = Response(request, self.schema_registry_url)
            responses.append(response)

        for i_module,module in enumerate(self.modules):
            log.info(f"Processing request for module: {module}")
            responses = module.process(responses)
            if i_module<len(self.modules)-1:#we set the responses to be processed (by the following module)             
                for response in responses:
                    response.set_as_to_be_processed()
            for response in responses:
                log.debug(f"response.to_dict(): {json.dumps(response.to_dict(), indent=2)}")
        return responses,end_flag
