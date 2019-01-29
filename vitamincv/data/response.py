import os
import sys
import json

from vitamincv.data.data import ModuleData
from vitamincv.data.avro_response.avro_response import AvroResponse

class Response():
    def __init__(self, bin_encoding=True, bin_decoding=True, 
                 prev_response=None, prev_response_url=None):
        """Response interface for converting to/from moduledata/response and serialization
        """
        if prev_response and prev_response_url:
            raise ValueError(f"request contains both prev_response and prev_response_url")
        
        log.info("Request contains previous response url")
        self.avro_response = AvroResponse()
        if prev_response:
            self.avro_response.set_doc(prev_response)
        elif prev_response_url:
            self.avro_response.set_doc(prev_response_url) #TODO

    def response_to_moduledata(properties_of_interest=None):
        """Convert response data to ModuleData type

        Args:
            properties_of_interest (dict): dictionary with properties of interest
        """
        dets = self._get_detections_from_response(properties_of_interest)
        segs = self._get_segments_from_response() #TODO
        return ModuleData(detections=dets, segments=segs)

    def moduledata_to_response(self, module_data):
        """Iterate over ModuleData that has been populated by the child,
           and populate response document
        
        Args:
            module_data (ModuleData)
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

    def _get_detections_from_response(self, prev_pois):
        if len(prev_pois)==0:
            log.info("No previous properties of interest to be searched.")
            return None
        if len(prev_pois): #properties of interested were defined in the child, or by means of the request.
            log.debug("There are previous properties of interest to be searched.")
            dets = []
            try:
                dets = self.avro_response.get_detections_from_props(prev_pois)
                log.debug("len(self.detections_of_interest): " + str(len(dets)))
                return dets
            except Exception as e:
                log.warning(e)