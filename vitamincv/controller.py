"""Processor of messages"""
import os
import sys
import glog as log
from abc import ABC, abstractmethod

from vitamincv.module.module import Module
from vitamincv.data.request import Request
from vitamincv.data.avro_response.avro_response import AvroResponse


class Controller():
    def __init__(self, modules):
        """Controller receives a Request and controls the communication to the CVmodules for processing,
            then returns a response
        """
        self.modules = modules
        self.modules_info = [{"name": x.name, "version": x.version} for x in modules]

    def process_request(self, request):
        """Send request_message through all the cvmodules

        Args:
            request (Request): incoming request

        Returns:
            Response: outgoing response message
        """
        if not isinstance(request, Request):
            raise ValueError(f"request is of type {type(request)}, not Request")
        
        response = None
        if request.prev_response:
            log.info("Loading from prev_response")
            response = AvroResponse(request.prev_response)
        else:
            log.info("No prev_response")
            response = AvroResponse()
        
        if request.prev_response_url:
            raise NotImplementedError()

        for module in self.modules:
            log.info(f"Processing request for module: {type(module)}")
            prev_module_data = response.response_to_mediadata(module.get_prev_props_of_interest())
            code = module.process(request, prev_module_data)
            log.info(f"{module.name} created {len(module.media_data.detections)} detections and {len(module.media_data.segments)} segments")
            response.mediadata_to_response(module.media_data)

        return response

