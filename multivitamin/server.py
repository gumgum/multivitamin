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
from responses_buffer import ResponsesBuffer

HEALTHPORT = os.environ.get("PORT", 5000)


class Server(Flask,ResponsesBuffer):
    def __init__(
        self, 
        modules,
        input_comm,
        n_parallelism=1,
        enable_parallelism=True,
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
        super(Flask, self).__init__(__name__)
        super(ResponsesBuffer,self).__init__(n=n_parallelism,enable_parallelism=enable_parallelism)
        
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
        self._pulling_thread=None
		self._pulling_thread_creation_time=None
		self._pushing_thread=None
		self._pushing_thread_creation_time=None

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

    def _start(self):
        """Start server. While loop that pulls requests from the input_comm, calls
        _process_request(request), and posts responses to output_comms
        """ 
        while True:
            try:
                self._pull_requests()
                self._process_requests()
                self._push_responses()
            except Exception:
                log.error(traceback.format_exc())
                log.error(f"Error processing requests: {requests}")

    def _pull_requests(self):
        if not self._enable_parallelism:
            self._pull_requests_thread_safe()
        else:
            if not self._pulling_thread:
                self._pulling_thread_creation_time=time.time()
                log.debug("Creating thread at " + str(self._pulling_thread_creation_time))  
                self._pulling_thread=Thread(group=None, target=self._pull_requests_thread_safe, name=None)
                self._pulling_thread.start()
                log.debug("Thread created")
            else:
                log.warning("We shouldn't be here")

    def _pull_requests_thread_safe(self):
        n=self.get_required_number_requests()
        requests = self.input_comm.pull(n)
        for request in requests:
            response = Response(request, self.schema_registry_url)
            self.add_response(response)

    def _push_responses(self):
        if not self._enable_parallelism:
            self._push_responses_thread_safe()
        else:
            if not self._pushing_thread:
                self._pushing_thread_creation_time=time.time()
                log.debug("Creating thread at " + str(self._pushing_thread_creation_time))  
                self._pushing_thread=Thread(group=None, target=self._push_responses_thread_safe, name=None)
                self._pushing_thread.start()
                log.debug("Thread created")
            else:
                log.warning("We shouldn't be here")

    def _push_responses_thread_safe(self):
        responses=get_responses_to_be_pushed()        
        for response in responses:
            response._push(self.output_comms)
        n_del= self.clean_pushed_responses()
        log.info(str(n_del) + " responses deleted, they were already pushed")

    def _process_requests(self):        
        for i_module,module in enumerate(self.modules):
            log.info(f"Processing request for module: {module}")
            last_module_flag = (i_module==len(self.modules)-1)
            responses=self.get_responses_ready_to_be_processed()
            if responses:
                module.process(responses)
            else:
                log.warning("No messages to be processed among " + str(self.get_current_number_responses))
            for response  in responses:
                if last_module_flag:
                    response.set_as_processed()
                else:
                    response.set_as_ready_to_be_processed()
            for response in responses:
                log.debug(f"response.to_dict(): {json.dumps(response.to_dict(), indent=2)}")
        return
