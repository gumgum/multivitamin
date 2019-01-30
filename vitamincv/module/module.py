import json
import glog as log
import datetime
import os
import traceback
from abc import ABC, abstractmethod

from vitamincv.media import MediaRetriever
from vitamincv.data.request import Request
from vitamincv.data import MediaData, create_metadata
from vitamincv.data.utils import p0p1_from_bbox_contour
from vitamincv.module.utils import list_contains_only_none

class Module(ABC):
    def __init__(self, server_name, version, prop_type=None,
                 prop_id_map=None, module_id_map=None):
        """Abstract base class that defines interface inheritance

        ImageModule, PropertiesModule

        Handles processing of request and previous media_data
        """
        self.name = server_name
        self.version = version
        self.prop_type = prop_type
        self.prop_id_map = prop_id_map
        self.module_id_map = module_id_map
        self.prev_pois = None
        self.media_data = MediaData(meta=create_metadata(self.name, self.version), 
                                      prop_id_map=prop_id_map, module_id_map=module_id_map)
        self.code='SUCCESS' #TODO

    def set_prev_props_of_interest(self, pois):
        self.prev_pois = pois
    
    def get_prev_props_of_interest(self):
        return self.prev_pois

    @abstractmethod
    def process(self, request, prev_media_data=None):
        assert isinstance(request, Request)
        self.request = request
        self.prev_media_data = prev_media_data
        self.media_data.meta["url"] = request.url

