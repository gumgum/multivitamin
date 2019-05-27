from abc import ABC, abstractmethod
import json
from itertools import filterfalse

import glog as log

from multivitamin.data import Response
from multivitamin.data.response.dtypes import Footprint
from multivitamin.data.response.utils import get_current_time
from multivitamin.module.codes import Codes
from multivitamin.module.utils import convert_props_to_pandas_query

class Module(ABC):
    def __init__(
        self, server_name, version, prop_type=None, prop_id_map=None, module_id_map=None, to_be_processed_buffer_size=1
    ):
        """Abstract base class that defines interface inheritance

        ImageModule, PropertiesModule

        Handles processing of request and previous response
        """        
        self.name = server_name
        self.version = version
        self.prop_type = prop_type
        self.prop_id_map = prop_id_map
        self.module_id_map = module_id_map
        self.to_be_processed_buffer_size=to_be_processed_buffer_size
        self.prev_pois = None
        self.code = Codes.SUCCESS
        self.prev_regions_of_interest_count = 0
        self.tstamps_processed = []
        self.responses_to_be_processed = []
        self.responses = []

    def get_required_number_requests(self):
        return self.to_be_processed_buffer_size-len(self.responses_to_be_processed)

    def set_prev_props_of_interest(self, pois):
        """If this Module is meant to be one in a sequence of Modules and is looking for a 
        particular set of properties of interest from the previous module.

        E.g.
            [
                {"property_type":"object", "value":"face"}, 
                {"value":"car"}
            ]
            
            is equivalent to
            
            '(property_type == "object") & (value == "face") | (value == "car")'
        
        Args:
            pois (list[dict]): previous properties of interest
        """
        assert isinstance(pois, list)
        for poi in pois:
            assert isinstance(poi, dict)

        self.prev_pois = pois
        self.prev_pois_bool_exp = convert_props_to_pandas_query(pois)
        log.info(
            f"Setting previous properties of interest: {json.dumps(pois, indent=2)}"
        )
        log.info(f"bool exp: {self.prev_pois_bool_exp}")

    def get_prev_props_of_interest(self):
        """Getter for properties of interest

        Returns:
            list[dict]: prev props of interest
        """
        return self.prev_pois

    @abstractmethod
    def process(self, responses):
        """Abstract method, public entry point for procesing a response

        Args:
            responses list[Response]: responses
        """               
        assert isinstance(responses, list)                
        for r in responses:                             
            r.tstamps_processed = []
            r.code = Codes.SUCCESS            
            r.set_as_to_be_processed()
            self.responses_to_be_processed.append(r)


    def update_and_return_response(self,response):
        """Update footprints, moduleID, propertyIDs

        Note: to be moved into aigumgum

        Returns:
            Response: response
        """
        log.info(f"Updating and returning response with code: {response.code.name}")
        num_footprints = len(response.footprints)
        time = get_current_time()
        log.debug("Appending footprints")
        response.append_footprint(
            Footprint(
                code=response.code.name,
                server=self.name,
                date=time,
                ver=self.version,
                id="{}{}".format(time, num_footprints + 1),
                tstamps=response.tstamps_processed,
                num_images_processed=len(response.tstamps_processed)
            )
        )
        self._update_ids(response)
        return response


    def update_and_return_responses(self):
        """Update footprints, moduleID, propertyIDs

        Note: to be moved into aigumgum

        Returns:
            list[Response]: output responses
        """
        #we clean self.responses
        self.responses = []
        #we mark as processed, with a timeout code, the responses from self.responses_to_be_processed that have been there for too long. 
        for r in self.responses_to_be_processed:
            r.check_timeout()
        #we move from to_be_processed to processed the responses already processed
        for r in self.responses_to_be_processed:
            if r.is_already_processed():#if de the children doesn't activate the request's FSM, this will always returns true
                self.responses.append(r)
        #we remove from to_be_processed the responses already processed
        self.responses_to_be_processed = [x for x in self.responses_to_be_processed if not x.is_already_processed()]
        

        #we update the responses
        for r in self.responses:                          
            self.update_and_return_response(response=r)
        return self.responses

    def _update_ids(self,response):
        """Internal method for updating property id and module id 
           in frame_anns and tracks
        """
        for image_ann in response.frame_anns:
            for region in image_ann["regions"]:
                for prop in region["props"]:
                    self._update_property_id(prop)
                    self._update_module_id(prop)

        for video_ann in response.tracks:
            for prop in video_ann["props"]:
                self._update_property_id(prop)
                self._update_module_id(prop)

        for video_ann in response.media_summary:
            for prop in video_ann["props"]:
                self._update_property_id(prop)
                self._update_module_id(prop)

    def _update_property_id(self, prop):
        """Internal method for updating property id in a property
        """
        log.debug("Updating property ids")
        if self.prop_id_map is None:
            return
        if prop["property_id"] == 0:
            value = prop.get("value", "")
            prop["property_id"] = self.prop_id_map.get(value, 0)

    def _update_module_id(self, prop):
        """Internal method for updating module id in a property
        """
        log.debug("Updating module ids")        
        if self.module_id_map is None:
            return
        if prop["module_id"] == 0:
            server = prop.get("server", "")
            prop["module_id"] = self.module_id_map.get(server, 0)

    def __repr__(self):
        return f"{self.name} {self.version}"
