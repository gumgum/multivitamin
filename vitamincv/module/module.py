from abc import ABC, abstractmethod
import json

import glog as log

from vitamincv.data.response import Response
from vitamincv.data.response.data import create_footprint
from vitamincv.data.response.utils import get_current_time
from vitamincv.module.codes import Codes
from vitamincv.module.utils import convert_list_of_query_dicts_to_bool_exp


class Module(ABC):
    def __init__(self, server_name, version, prop_type=None, prop_id_map=None, module_id_map=None):
        """Abstract base class that defines interface inheritance

        ImageModule, PropertiesModule

        Handles processing of request and previous response
        """
        self.name = server_name
        self.version = version
        self.prop_type = prop_type
        self.prop_id_map = prop_id_map
        self.module_id_map = module_id_map
        self.prev_pois = None
        self.code = Codes.SUCCESS
        self.prev_regions_of_interest_count = 0

    def set_prev_props_of_interest(self, pois):
        """docs"""
        self.prev_pois = pois
        self.prev_pois_bool_exp = convert_list_of_query_dicts_to_bool_exp(pois)
        log.info(f"Setting previous properties of interest: {json.dumps(pois, indent=2)}")
        log.info(f"bool exp: {self.prev_pois_bool_exp}")

    def get_prev_props_of_interest(self):
        return self.prev_pois

    @abstractmethod
    def process(self, response):
        assert isinstance(response, Response)
        self.response = response

    def update_and_return_response(self):
        """Update footprints, moduleID, propertyIDs
        """
        num_footprints = len(self.response.footprints)
        time = get_current_time()
        self.response.append_footprint(
            create_footprint(
                code=self.code.name, 
                server=self.name,
                date=time,
                ver=self.version, 
                id="{}{}".format(time, num_footprints+1),
                tstamps=self.response.timestamps
                )
            )
        self.response.url_original = self.response.url
        self._update_ids()
        return self.response

    def _update_ids(self):
        for tstamp, regions in self.response.frame_anns.items():
            for region in regions:
                for prop in region["props"]:
                    if prop["module_id"] == 0:
                        prop["module_id"] = self.module_id_map[prop["server"]]
                    if prop["property_id"] == 0:
                        prop["property_id"] = self.prop_id_map[prop["value"]]
        
        for video_ann in self.response.tracks():
            for prop in video_ann["props"]:
                if prop["module_id"] == 0:
                    prop["module_id"] = self.module_id_map[prop["server"]]
                if prop["property_id"] == 0:
                    prop["property_id"] = self.prop_id_map[prop["value"]] 


    def __repr__(self):
        return f"{self.name} {self.version}"
