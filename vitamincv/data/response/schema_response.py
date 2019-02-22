# Wrapper around Response
# constructor and setter has conversion to schema response

import glog as log

from vitamincv.data.response import Response


class SchemaResponse:
    def __init__(self, response):
        log.info("Constructing SchemaResponse from Response")
        assert isinstance(response, Response)
        self._schema_response = None
        self.schema_response = response

    @property
    def schema_response(self):
        return self._schema_response

    @schema_response.setter
    def schema_response(self, response):
        assert isinstance(response, Response)
        self._schema_response

    def _to_schema_response(self):
        pass

    # def convert_to_schema(self):
    #     """CURRENTLY USED FOR COMPATIBILITY W/ SCHEMA. SHOULD CHANGE SCHEMA TO REFLECT THIS RESPONSE, THEN REMOVE THIS METHOD"""
    #     return None
