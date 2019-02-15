from abc import ABC, abstractmethod
import json

from vitamincv.data.request import Request
from vitamincv.data.response import Response
from vitamincv.module.codes import Codes
from vitamincv.module.utils import convert_list_of_query_dicts_to_pd_query

import glog as log


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

    def set_prev_props_of_interest(self, pois):
        """docs"""
        self.prev_pois = pois
        self.pd_query_prev_pois = convert_list_of_query_dicts_to_pd_query(pois)
        log.info(f"Setting previous properties of interest: {json.dumps(pois, indent=2)}")
        log.info(f"Pandas query str: {self.pd_query_prev_pois}")

    def get_prev_props_of_interest(self):
        return self.prev_pois

    @abstractmethod
    def process(self, request, response):
        assert isinstance(request, Request)
        assert isinstance(response, Response)
        self.request = request
        self.response = response

    def __repr__(self):
        return f"{self.name} {self.version}"
