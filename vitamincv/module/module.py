from abc import ABC, abstractmethod

from vitamincv.data.request import Request
from vimtaincv.data.response import AvroResponse
from codes import Codes


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
        self.code = Codes.SUCCESS.name

    def set_prev_props_of_interest(self, pois):
        self.prev_pois = pois

    def get_prev_props_of_interest(self):
        return self.prev_pois

    @abstractmethod
    def process(self, request, prev_response=None):
        assert(isinstance(request, Request))
        assert(isinstance(prev_response, AvroResponse))
        self.request = request
        self.prev_response = prev_response
        self.response = AvroResponse()

    def __repr__(self):
        return f"{self.name} {self.version}"