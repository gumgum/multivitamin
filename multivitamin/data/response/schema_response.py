import copy

import glog as log

from multivitamin.data.request import Request
from multivitamin.data.response import ModuleResponse
from multivitamin.data.response.io import AvroIO


class SchemaResponse:
    def __init__(self, 
                 input, 
                 use_schema_registry=True
                 ):
        """ SchemaResponse is an adapter for Response,
            adapting Response to our schema 
            
            Also provides methods for serialization/deserialization
        
            This adapter is intended to maintain the contract of the schema
            All CommAPIs interact w/ SchemaResponse (not Response)

            There are 2 cases where SchemaResponse should be constructed:

                1) constructed from a request with a previous (schema) response
                2) converting a Response to SchemaResponse for sending data to client

        Args:
            input (Response): previous response
            request (Request): incoming request
            dictionary (dict): schema response dictionary
            use_schema_registry (bool): whether to use schema registry when serializing to bytes
        """
        self._use_schema_registry = use_schema_registry
        self._dictionary = None
        self._request = None

        if isinstance(input, ModuleResponse):
            log.info("Constructing from ModuleResponse")
            self._init_from_module_response(input)
        elif isinstance(input, Request):
            log.info("Constructing from Request")
            self._init_from_request(input)
        elif isinstance(input, dict):
            log.info("Constructing from SchemaResponse dictionary")
            self.dict = input
        else:
            raise ValueError(f"Input must be ModuleResponse, Request, or dict. Found {type(input)}")

    @property
    def dict(self):
        log.info("Returning schema response as dictionary")
        if self._request is not None:
            if self._request.bin_encoding is True:
                log.warning("self._request.bin_encoding is True but returning dictionary")
        return self._dictionary

    @property
    def bytes(self):
        log.info("Returning schema response as binary")
        log.info(f"base64 encoding: {self._request.base64_encoding}")
        try:
            io = AvroIO(self._use_schema_registry)
            return io.encode(self._dictionary, self._request.base64_encoding)
        except Exception:
            log.error("Error serializing response")
            # what to do here?
            raise Exception("Error serializing response")

    @property
    def data(self):
        """Convenience method to return either dict or bytes depending on request
        """
        if self._request is None:
            return self._dictionary

        if self._request.bin_encoding is False:
            log.info("Returning schema response as dictionary")
            return self._dictionary

        # else: return serialized bytes
        return self.bytes

    @dict.setter
    def dict(self, d):
        """ Dictionary setter for SchemaResponse

            Checks schema validity 

            Args:
                d (dict): dictionary
        """
        if d is not None:
            assert(isinstance(d, dict))
            io = AvroIO(self._use_schema_registry)
            if not io.is_valid_avro_doc(d):
                raise ValueError(
                    "schema response dict is incompatible with avro schema"
                    )
            else:
                log.info("dict is COMPATIBLE with avro schema")
        self._dictionary = d

    @property
    def request(self):
        return self._request

    @property
    def url(self):
        if self._dictionary is None:
            return None
        return self._dictionary["media_annotation"]["url"]

    def to_module_response(self):
        """ Convert schema_response._dictionary to ModuleResponse and return
        """
        log.info("Converting schema_response._dictionary to a ModuleResponse")
        assert(self._request is not None)
        if self._dictionary is None:
            log.info("No prev_response")
            return ModuleResponse(request=self._request)
    
        log.info("Non empty prev_response")
        log.info("Converting frame_anns list to frame_anns dict")

        response_dict = copy.deepcopy(self._dictionary)
        frame_anns = self._dictionary.get("media_annotation").get("frames_annotation")
        assert(isinstance(frame_anns, list))
        frame_anns_dict = {image_ann["t"]: image_ann["regions"] for image_ann in frame_anns}

        response_dict["media_annotation"]["frames_annotation"] = frame_anns_dict
        return ModuleResponse(dictionary=response_dict, request=self._request)

    def _init_from_request(self, request):
        """Construct from a request. Request object has a field for "prev_response"
        If not None, convert prev_response into a SchemaResponse
        
        prev_responses can come in the following forms

            1) prev_response_url
                a) binary
                b) base64 binary string
                c) string (json file)
                i) local file
                ii) remote file

            2) prev_response
                a) binary
                b) base64 binary string
                c) dict
        
        Args:
            request (Request): request obj
        """
        self._request = request
        log.info("Constructing SchemaResponse from Request")
        if request.prev_response:
            log.info("Loading from prev_response")
            if request.bin_encoding is True:
                log.info("bin_encoding is True")
                io = AvroIO()
                if isinstance(request.prev_response, str):
                    log.info("prev_response is base64 encoded binary")
                    bytes = io.decode(request.prev_response, use_base64=True, binary_flag=True)
                else:
                    log.info("prev_response is in binary")
                    bytes = io.decode(request.prev_response, use_base64=False, binary_flag=True)
                self.dict = io.decode(bytes)
            else:
                log.info("prev_response is a dict")
                self.dict = request.prev_response
        elif self._request.prev_response_url:
            log.info("Loading from prev_response_url")
            raise NotImplementedError()
        else:
            log.info("No prev_response")

    def _init_from_module_response(self, module_response):
        """Construct from a ModuleResponse

        Args:
            module_response (ModuleResponse): prev module response
        """
        self._request = module_response.request
        log.info("Converting module_response to schema_response")
        self._dictionary = copy.deepcopy(module_response.dict)
        log.info("Converting frame_anns dict to frame_anns list")
        frame_anns = module_response.frame_anns
        assert isinstance(frame_anns, dict)
        frame_anns_list = [
            {"t": tstamp, "regions": regions} for tstamp, regions in frame_anns.items()
            ]
        self._dictionary["media_annotation"]["frames_annotation"] = frame_anns_list






