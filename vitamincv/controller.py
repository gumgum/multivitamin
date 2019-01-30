"""Processor of messages"""
import os
import sys
import glog as log
from abc import ABC, abstractmethod

from vitamincv.module.module import Module
from vitamincv.data.request import Request
from vitamincv.data.avro_response.avro_response import AvroResponse


class Controller():
    def __init__(self, cvmodules):
        """Controller receives a Request and controls the communication to the CVmodules for processing,
            then returns a response
        """
        self._cvmodules = cvmodules
        self.modules_info = [{"name": x.name, "version": x.version} for x in cvmodules]

    def process_request(self, request):
        """Send request_message through all the cvmodules

        Args:
            request (Request): incoming request

        Returns:
            Response: outgoing response message
        """"
        if not isinstance(request, Request):
            raise ValueError(f"request_message is of type {type(request)}, not Request")
        
        response = None
        if request.prev_response:
            log.info("Loading from prev_response")
            response = AvroResponse(request.prev_response)
        else:
            response = AvroResponse()
        
        if request.prev_response_url:
            raise NotImplementedError()

        for module in self._cvmodules:
            log.info(f"Processing request for cvmodule: {type(module)}")
            prev_module_data = response.response_to_moduledata(module.get_prev_props_of_interest())
            cur_module_data = module.process(request, prev_module_data)
            response.moduledata_to_response(cur_module_data)

        if request.bin_encoding():
            return response.to_bytes()
        return response.to_dict()

