# Wrapper around Response
# constructor and setter has conversion to schema response

import glog as log
import copy

from vitamincv.data.request import Request
from vitamincv.data.response import Response
from vitamincv.data.response.io import AvroIO


class SchemaResponse():
    def __init__(self, dictionary=None, request=None):
        self._dictionary = dictionary
        self.request = request
    
    @property
    def url(self):
        if self._dictionary is None:
            return None
        return self._dictionary["media_annotation"]["url"]
    
    @property
    def dictionary(self):
        return self._dictionary
    
    @dictionary.setter
    def dictionary(self, dct):
        io = AvroIO(use_base64=self.request.base64_encoding)
        if not io.is_valid_avro_doc(dct):
            raise ValueError("dict does not match avro schema")
        self._dictionary = dct


class SchemaResponseConverter():
    def __init__(self):
        self._schema_response = None
        self._response = None

    def construct_from_request(self, request):
        """
        Prev response can come in the following forms

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
        """
        log.info("Constructing SchemaResponse from Response")
        assert(isinstance(request, Request))

        schema_response_dict = None
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
                schema_response_dict = io.decode(bytes)
            else:
                assert(isinstance(request.prev_response, dict))
                log.info("prev_response is a dict")
                schema_response_dict = request.prev_response
        elif request.prev_response_url:
                log.info("Loading from prev_response_url")
                raise NotImplementedError()
        else:
            log.info("No prev_response")

        self._schema_response = SchemaResponse(dictionary=schema_response_dict, request=request)
        self._construct_response_from_schema_response()

    def construct_from_response(self, response):
        assert(isinstance(response, Response))
        self._response = response
        self._construct_schema_response_from_response()

    def get_schema_response(self, use_base64=False, binarize=False):
        if binarize is False:
            log.info("Returning schema response as dictionary")
            return self._schema_response
        
        io = AvroIO()
        return io.encode(self._schema_response, use_base64)

    def get_response(self):
        if self._response is None:
            log.info("self._response is None")
            raise ValueError()
        return self._response

    def _construct_response_from_schema_response(self):
        """Convert SchemaResponse to Response
        """
        log.info("construct_response_from_schema_response")
        if self._schema_response.dictionary is None: # no prev_response
            log.info("Empty prev_response")
            self._response = Response(request=self._schema_response.request)
        else:
            log.info("Not empty prev_response; converting frame_anns...")
            frame_anns = copy.deepcopy(self._schema_response.dictionary.get("media_annotation").get("frames_annotation"))
            assert(isinstance(frame_anns, list))
            frame_anns_dict = {image_ann['t']: image_ann['regions'] for image_ann in frame_anns}
            response_dict = copy.deepcopy(self._schema_response.dictionary)
            response_dict["media_annotation"]["frames_annotation"] = frame_anns_dict
            self._response = Response(dictionary=response_dict, request=self._schema_response.request)

    def _construct_schema_response_from_response(self):
        """Update schema response from self._response
        """
        log.info("Constructing schema_response from response")
        dct = copy.deepcopy(self._response.dictionary)
        frame_anns = dct.get("media_annotation").get("frames_annotation")
        frame_anns_list = [{'t': tstamp, 'regions': regions} for tstamp, regions in frame_anns.items()]
        dct["media_annotation"]["frames_annotation"] = frame_anns_list
        self._schema_response = SchemaResponse(dictionary=dct, request=self._response.request)

