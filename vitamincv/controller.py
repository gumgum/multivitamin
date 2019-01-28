"""Processor of messages"""
import os
import sys
import glog as log
from abc import ABC, abstractmethod

from vitamincv.module.cvmodule import CVModule

from vitamincv.data.request_message import Request

class Controller():
    def __init__(self):
        """Controller receives a Request and orchestrates the communication to the CVmodules for processing,
            then returns a response
        """
        self.converter = DataConverter()
        
    def process_request(self, request):
        """Send request_message through all the cvmodules

        Args:
            request_message (Request): incoming request
            previous_response_message (Response): previous response message
        Returns:
            Response: outgoing response message
        """"
        if not isinstance(request, Request):
            raise ValueError(f"request_message is of type {type(request)}, not Request")
        
        response = None
        if request.get_prev_response():
            log.info("Request contains previous response")
            response = Response(
                    bin_decoding=request.bin_decoding(), 
                    prev_response=request.get_prev_response()
                )

        if request.get_prev_response_url():
            if response is not None:
                raise ValueError(f"request contains both prev_response and prev_response_url")
            log.info("Request contains previous response url")
            response = Response(
                    bin_decoding=request.bin_decoding(), 
                    prev_response_url=request.get_prev_response_url()
                )

        for module in self._cvmodules:
            log.info(f"Processing request for cvmodule: {type(module)}")
            module_data = self.convert_response_to_moduledata(request, response, module)
            out_moduledata = module.process(request, module_data)
            response = self.convert_moduledata_to_response(out_moduledata)

        if request.bin_encoding():
            return response.to_bytes()
        return response.to_dict()

    def convert_response_to_moduledata(request, response, module):
        prev_props_of_interest = module.get_prev_props_of_interest()

    def _find_detections_from_pois(self):
        if len(self.prev_pois)==0:
            log.info("No previous properties of interest to be searched.")
            return None
        if len(self.prev_pois): #properties of interested were defined in the child, or by means of the request.
            log.debug("There are previous properties of interest to be searched.")
            try:
                self.detections_of_interest=self.avro_api.get_detections_from_props(self.prev_pois)
            except Exception as e:
                log.warning(e)
            log.debug("len(self.detections_of_interest): " + str(len(self.detections_of_interest)))
            log.debug("Creating self.detections_t_map.")
            
    def convert_moduledata_to_response(self, module_data):
        """Iterate over self.detections that has been populated by the child,
           and populate response document
        """
        log.info("Adding footprint.")
        date = get_current_time()
        n_footprints=len(self.avro_api.get_footprints())
        footprint_id=date+str(n_footprints+1)
        fp=create_footprint(code=self.code, ver=self.version, company="gumgum", labels=None, server_track="",
                     server=self.name, date=date, annotator="",
                     tstamps=None, id=footprint_id)
        self.avro_api.append_footprint(fp)
        log.info('self.code: ' + str(self.code))
        if self.code!='SUCCESS':
            log.error('The processing was not succesful')
            return
        self.avro_api.set_url(self.request_api.get_url())
        self.avro_api.set_url_original(self.request_api.get_url())
        self.avro_api.set_dims(*self.request_api.media_api.get_w_h())
       
        if self.process_properties_flag==False:
            module_id=0
            if self.module_id_map:
                module_id=self.module_id_map.get(self.name)
                if module_id is None:
                    log.warning('module '+ self.name +' is not in module_id_map.')
            log.info("len(self.detections): " + str(len(self.detections)))            
            for det in self.detections:
                value = det.get("value")
                if value is None:
                    log.warning("det['value'] should have a value at this point.")
                det['server'] = self.name
                det['module_id'] = module_id
                det['ver'] = self.version
                det['footprint_id'] = footprint_id
                try:
                    self.avro_api.append_detection(det,self.prop_id_map)
                except:
                    log.error("Problem appending detection.")
                    log.error(traceback.print_exc())
            self.detections=[]
        else:
            log.info("Not appending any detection, this was a property processor module")
        log.debug("We reset the queriers to None. If a caller performs a query they need to be rebuilt with the new detections and segments.")
        self.avro_api.reset_queriers()
        




