import json

import glog as log

from multivitamin.data import Request
from multivitamin.data.response.io import AvroIO
from multivitamin.data.response.data import (
    ResponseInternal,
    Region,
    VideoAnn,
    ImageAnn,
    Footprint,
)


class Response():
    def __init__(self, 
                 input, 
                 use_schema_registry=True
                 ):
        """ SerializableResponse is a wrapper for Response with methods 
            for serializable

            There are 2 cases where SerializableResponse should be constructed:

                1) constructed from a request with a previous SerializableResponse
                2) converting a Response to SchemaResponse for sending data to client

        Args:
            input (Response): previous response
            request (Request): incoming request
            dictionary (dict): schema response dictionary
            use_schema_registry (bool): whether to use schema registry when serializing to bytes
        """
        self._use_schema_registry = use_schema_registry
        self._request = None
        self._response_internal = None
        self._tstamp2frameannsidx = None

        if isinstance(input, Request):
            self._init_from_request(input)
        elif isinstance(input, dict):
            io = AvroIO(use_schema_registry)
            if not io.is_valid_avro_doc(input):
                raise ValueError(
                    "Input dict is incompatible with avro schema"
                )
            log.info("Input dict is compatible with avro schema")
            self._response_internal = ResponseInternal().from_dict(input)
        else:
            raise TypeError(f"Expected Request or dict, found type: {type(input)}")

    @property
    def dict(self):
        log.info("Returning schema response as dictionary")
        if self._request is not None:
            if self._request.bin_encoding is True:
                log.warning("self._request.bin_encoding is True but returning dictionary")
        return self._response_internal.to_dict()

    @property
    def bytes(self):
        log.info("Returning schema response as binary")
        log.info(f"base64 encoding: {self._request.base64_encoding}")
        try:
            io = AvroIO(self._use_schema_registry)
            return io.encode(self._response_internal.to_dict(), self._request.base64_encoding)
        except Exception:
            log.error("Error serializing response")
            # what to do here?
            raise Exception("Error serializing response")

    @property
    def data(self):
        """Convenience method to return either dict or bytes depending on request
        """
        if self._request is None:
            return self.dict

        if self._request.bin_encoding is False:
            log.info("Returning schema response as dictionary")
            return self.dict

        # else: return serialized bytes
        return self.bytes

    @property
    def tracks(self):
        return self._response_internal["media_annotation"]["tracks_summary"]

    @property
    def frame_anns(self):
        return self._response_internal["media_annotation"]["frames_annotation"]

    def has_frame_anns(self):
        return len(self.frame_anns) > 0

    @property
    def request(self):
        return self._request

    @property
    def url(self):
        return self._response_internal["media_annotation"]["url"]

    @url.setter
    def url(self, url):
        assert isinstance(url, str)
        self._response_internal["media_annotation"]["url"] = url

    @property
    def url_original(self):
        return self._response_internal["media_annotation"]["url_original"]

    @url_original.setter
    def url_original(self, url):
        assert isinstance(url, str)
        self._response_internal["media_annotation"]["url_original"] = url

    @property
    def media_summary(self):
        return self._response_internal["media_annotation"]["media_summary"]

    @property
    def footprints(self):
        return self._response_internal["media_annotation"]["codes"]

    @property
    def width(self):
        return self._response_internal["media_annotation"]["w"]

    @width.setter
    def width(self, w):
        assert isinstance(w, int)
        self._response_internal["media_annotation"]["w"] = w

    @property
    def height(self):
        return self._response_internal["media_annotation"]["h"]

    @height.setter
    def height(self, h):
        assert isinstance(h, int)
        self._response_internal["media_annotation"]["h"] = h

    @property
    def timestamps_from_frames_ann(self):
        return sorted(self._response_internal["media_annotation"]["frames_annotation"].keys())

    def timestamps(self, server=None):
        """TODO, cleanup?"""
        tstamps=[]        
        for c in self._response_internal["media_annotation"]["codes"]:
            #log.debug(str(c))
            if not c["tstamps"]:
                continue
            if server:
                if c["server"] != server:
                    continue
            if not tstamps:
                log.debug("Assigning timestamps: " + str(c["tstamps"]))
                tstamps=c["tstamps"]
            else:
                tstamps=list(set(tstamps) | set(c["tstamps"])) 
        return sorted(list(set(tstamps)))

### Modifiers

    def append_region(self, t, region):
        assert(isinstance(t, float))
        assert(isinstance(region, Region))
        if t in self._tstamp2frameannsidx:
            log.debug(f"t: {t} in frame_anns, appending Region")
            frame_anns_idx = self._tstamp2frameannsidx[t]
            self._response_internal["media_annotation"]["frames_annotation"][frame_anns_idx].append(region)
        else:
            log.debug(f"t: {t} NOT in frame_anns, appending ImageAnn")
            ia = ImageAnn(t=t, regions=[region])
            self._response_internal["media_annotation"]["frames_annotation"].append(ia)
            self._tstamp2frameannsidx[t] = len(self.frame_anns - 1)

    def append_regions(self, t, regions):
        assert(isinstance(t, float))
        assert(isinstance(regions, list))
        for region in regions:
            assert(isinstance(region, Region))
        
        if t in self._tstamp2frameannsidx:
            log.debug(f"t: {t} in frame_anns, extending Regions")
            frame_anns_idx = self._tstamp2frameannsidx[t]
            self._response_internal["media_annotation"]["frames_annotation"][frame_anns_idx].extend(regions)
        else:
            log.debug(f"t: {t} NOT in frame_anns, appending ImageAnn")
            ia = ImageAnn(t=t, regions=regions)
            self._response_internal["media_annotation"]["frames_annotation"].append(ia)
            self._tstamp2frameannsidx[t] = len(self.frame_anns - 1)

    def append_footprint(self, footprint):
        assert(isinstance(footprint, Footprint))
        self._response_internal["media_annotation"]["codes"].append(footprint)

    def append_track(self, video_ann):
        assert(isinstance(video_ann, VideoAnn))
        self._response_internal["media_annotation"]["tracks_summary"].append(video_ann)

    def append_media_summary(self, video_ann):
        assert(isinstance(video_ann, VideoAnn))
        self._response_internal["media_annotation"]["media_summary"].append(video_ann)

    def sort_image_anns_by_timestamp(self):
        tmp = self._response_internal["media_annotation"]["frames_annotation"]
        self._response_internal["media_annotation"]["frames_annotation"] = sorted(
            tmp, key=lambda k: k["t"]
        )

    def sort_tracks_summary_by_timestamp(self):
        tmp = self._response_internal["media_annotation"]["tracks_summary"]
        self._response_internal["media_annotation"]["tracks_summary"] = sorted(
            tmp, key=lambda k: k["t1"]
        )

    def _init_from_request(self):
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
        log.info("Constructing SchemaResponse from Request")
        if self._request.prev_response:
            log.info("Loading from prev_response")
            if self._request.bin_encoding is True:
                log.info("bin_encoding is True")
                io = AvroIO()
                if isinstance(self._request.prev_response, str):
                    log.info("prev_response is base64 encoded binary")
                    bytes = io.decode(self._request.prev_response, use_base64=True, binary_flag=True)
                else:
                    log.info("prev_response is in binary")
                    bytes = io.decode(self._request.prev_response, use_base64=False, binary_flag=True)
                log.info("Constructing ResponseInternal")
                d = io.decode(bytes)
                self._response_internal = ResponseInternal().from_dict(d)
            else:
                assert(isinstance(self._request.prev_response, str))
                log.info("prev_response is a JSON str")
                d = json.loads(self._request.prev_response)
                self._response_internal = ResponseInternal().from_dict(d)
        elif self._request.prev_response_url:
            log.info("Loading from prev_response_url")
            raise NotImplementedError()
        else:
            log.info("No prev_response")






