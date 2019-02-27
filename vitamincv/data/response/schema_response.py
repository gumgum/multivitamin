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
            raise ValueError("dict is incompatible with avro schema")
        self._dictionary = dct

    @property
    def data(self):
        """Data to be sent in a message
        """
        if self.request.bin_encoding is False:
            log.info("Returning schema response as dictionary")
            return self._dictionary
        
        log.info("Returning schema response as binary")
        log.info(f"base64 encoding: {self.request.base64_encoding}")
        io = AvroIO()
        return io.encode(self._dictionary, self.request.base64_encoding)

def request_to_schema_response(request):
    """Prev response can come in the following forms

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

    return SchemaResponse(dictionary=schema_response_dict, request=request)

def schema_response_to_response(schema_response):
    """
    """
    log.info("Converting schema_response to response")
    assert(isinstance(request, Response))

    if schema_response.dictionary is None: 
        log.info("Empty prev_response")
        return Response(request=self._schema_response.request)
    
    log.info("Non empty prev_response")
    log.info("Converting frame_anns list to frame_anns dict")
    
    frame_anns = copy.deepcopy(self._schema_response.dictionary.get("media_annotation").get("frames_annotation"))
    assert(isinstance(frame_anns, list))
    frame_anns_dict = {image_ann['t']: image_ann['regions'] for image_ann in frame_anns}
    response_dict = copy.deepcopy(self._schema_response.dictionary)
    response_dict["media_annotation"]["frames_annotation"] = frame_anns_dict
    return Response(dictionary=response_dict, request=self._schema_response.request)

def response_to_schema_response(response):
    """
    """
    log.info("Converting response to schema_response")
    log.info("Converting frame_anns dict to frame_anns list")

    frame_anns = response.dictionary.get("media_annotation").get("frames_annotation")
    assert(isinstance(frame_anns, dict))
    frame_anns_list = [{'t': tstamp, 'regions': regions} for tstamp, regions in frame_anns.items()]
    response["media_annotation"]["frames_annotation"] = frame_anns_list
    return SchemaResponse(dictionary=response, request=self._response.request)




