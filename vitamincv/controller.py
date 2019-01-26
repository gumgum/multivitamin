"""Processor of messages"""
import os
import sys
import glog as log
from abc import ABC, abstractmethod

from vitamincv.module.cvmodule import CVModule

from vitamincv.data.request_message import Request

class Processor(ABC):
    def __init__(self, server_name, version, prop_type=None, prop_id_map=None, 
                 module_id_map=None, process_properties_flag=False):
        """Abstract base class for CVModules
        
        Args:
            server_name (str): name of server/module
            version (str): version of module
            idmap (str): filepath to idmap
        """
        log.info('Constructing cvmodule')
        self.name = server_name
        self.version = version
        self.prop_type = prop_type
        self.prop_id_map = prop_id_map
        self.module_id_map = module_id_map

        self.process_properties_flag = process_properties_flag
        self.prev_pois = [] #this are the properties of interest that define the regions of interest, the ones that we'll want to analyze.
        self.detections_of_interest=[]#A list with the detections of interest, the ones that we'll want to analyze.
        self.code='SUCCESS'
        self.num_problematic_frames=0
        self.detections=[]
    
    def _process_module(self, cvmodule, request, response):
        """Process request message and append to previous response"""
        codes = cvmodule.process(request)
        log.info("Updating response")
        response = self.convert_to_response(cvmodule.get_output())

            request=m.get_request_api()
            if i<len(self.modules)-1:
                log.info("Reseting media_api in the requests.")
                if isinstance(request,list):
                    for r in request:
                        r.reset_media_api()
                else:
                    request.reset_media_api()

    def process():
        pass

from vitamincv.media.media import MediaRetriever

class ImageProcessor(Processor):
    def __init__(self):
        pass
    

class PropertiesProcessor(Processor):
    def __init__(self):
        pass

    def convert_response_to_cvdata(response_message):
        print("hi")

    def convert_cvdata_to_response(cvdata):
        print("ho")


    def update_response(self):
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