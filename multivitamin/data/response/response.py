import json
import traceback

import glog as log
from dataclasses import asdict

from multivitamin.data import Request
from multivitamin.data.response.io import AvroIO
from multivitamin.data.response.dtypes import (
    ResponseInternal,
    Region,
    VideoAnn,
    ImageAnn,
    Footprint,
)


class Response:
    def __init__(self, response_input, use_schema_registry=True):
        """ Class for a Response object
        
        2 cases for construction:

                1) constructed from a request with a previous Response
                2) constructing from a dictionary

        Args:
            input (Any): previous Response or dict
            use_schema_registry (bool): whether to use schema registry when serializing to bytes
        """
        self._use_schema_registry = use_schema_registry
        self._request = None
        self._response_internal = None
        self._tstamp2frameannsidx = {}

        if isinstance(response_input, Request):
            self._request = response_input
            self._init_from_request()
        elif isinstance(response_input, dict):
            io = AvroIO(use_schema_registry)
            if not io.is_valid_avro_doc(response_input):
                raise ValueError("Input dict is incompatible with avro schema")
            log.debug("Input dict is compatible with avro schema")

            # unpack dictionary values into kwargs using ** operator
            try:
                self._response_internal = ResponseInternal(**response_input) 
            except Exception as e:
                log.error("error unpacking prev_response_dict")
                log.error(traceback.format_exc())
        else:
            raise TypeError(
                f"Expected Request or dict, found type: {type(response_input)}"
            )

    def to_dict(self):
        """Getter for response in the form of a dict

        Returns:
            dict: response as dict
        """
        log.debug("Returning response as dictionary")
        if self._request is not None:
            if self._request.bin_encoding is True:
                log.warning(
                    "self._request.bin_encoding is True but returning dictionary"
                )
        return asdict(self._response_internal)

    def to_bytes(self, base64=False):
        """Getter for response in the form of bytes or base64 str

        Args:
            base64 (bool): flag for converting to base64 encoding
        
        Returns:
            bytes: response as bytes
        """
        if self._request is not None:
            base64 = self._request.base64_encoding
            log.info("Using self._request to check base64_encoding flag")
        log.debug(f"base64 encoding: {base64}")
        log.debug("Returning response as binary")
        try:
            io = AvroIO(self._use_schema_registry)
            return io.encode(asdict(self._response_internal), base64)
        except Exception:
            log.error("Error serializing response")
            # what to do here?
            raise Exception("Error serializing response")

    @property
    def data(self):
        """Convenience getter to return either dict or bytes depending on request

        Returns:
            Any: dict or bytes
        """
        if self._request is None:
            return self.to_dict()

        if self._request.bin_encoding is False:
            log.debug("Returning response as dictionary")
            return self.to_dict()

        return self.to_bytes(self._request.base64_encoding)

    @property
    def tracks(self):
        """Getter for returning tracks

        Returns:
            List[VideoAnn]: tracks
        """
        return self._response_internal["media_annotation"]["tracks_summary"]

    @property
    def frame_anns(self):
        """Getter for frames annotations

        Returns:
            List[ImageAnn]: frames ann
        """
        return self._response_internal["media_annotation"]["frames_annotation"]

    def has_frame_anns(self):
        """Bool check on whether there are frame anns

        Returns:
            bool: flag for existence of frame anns
        """
        return len(self.frame_anns) > 0

    def get_regions_from_tstamp(self, t):
        """Get regions for a timestamp

        Args:
            t (float): tstamp
        
        Returns:
            List[Region]: regions
        """
        assert isinstance(t, float)
        if t not in self._tstamp2frameannsidx:
            return None
        frame_anns_idx = self._tstamp2frameannsidx[t]
        return self._response_internal["media_annotation"]["frames_annotation"][frame_anns_idx]["regions"]

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

    def get_timestamps_from_frames_ann(self):
        return sorted(self._tstamp2frameannsidx.keys())

    def get_timestamps(self, server=None):
        """TODO, cleanup?"""
        tstamps = []
        for c in self._response_internal["media_annotation"]["codes"]:
            # log.debug(str(c))
            if not c["tstamps"]:
                continue
            if server:
                if c["server"] != server:
                    continue
            if not tstamps:
                log.debug("Assigning timestamps: " + str(c["tstamps"]))
                tstamps = c["tstamps"]
            else:
                tstamps = list(set(tstamps) | set(c["tstamps"]))
        return sorted(list(set(tstamps)))

    # Modifiers

    def append_region(self, t, region):
        """Append a region given a timestamp. 

        If tstamp exists in frame_anns, appends region, otherwise, creates new ImageAnn

        Args:
            t (float): tstamp
            region (Region): region
        """
        assert isinstance(t, float)
        assert isinstance(region, type(Region()))
        if t in self._tstamp2frameannsidx:
            log.debug(f"t: {t} in frame_anns, appending Region")
            frame_anns_idx = self._tstamp2frameannsidx[t]
            self._response_internal["media_annotation"]["frames_annotation"][frame_anns_idx]["regions"].append(region)
        else:
            log.debug(f"t: {t} NOT in frame_anns, appending ImageAnn")
            ia = ImageAnn(t=t, regions=[region])
            self._response_internal["media_annotation"]["frames_annotation"].append(ia)
            self._tstamp2frameannsidx[t] = len(self.frame_anns) - 1

    def append_regions(self, t, regions):
        """Append a list of regions given a timestamp

        If tstamp exists in frame_anns, appends regions, otherwise, creates new ImageAnns

        Args:
            t (float): tstamp
            region (Region): region
        """
        assert isinstance(t, float)
        assert isinstance(regions, list)
        for region in regions:
            assert isinstance(region, type(Region()))

        if t in self._tstamp2frameannsidx:
            log.debug(f"t: {t} in frame_anns, extending Regions")
            frame_anns_idx = self._tstamp2frameannsidx[t]
            self._response_internal["media_annotation"]["frames_annotation"][frame_anns_idx].extend(regions)
        else:
            log.debug(f"t: {t} NOT in frame_anns, appending ImageAnn")
            ia = ImageAnn(t=t, regions=regions)
            self._response_internal["media_annotation"]["frames_annotation"].append(ia)
            self._tstamp2frameannsidx[t] = len(self.frame_anns) - 1

    def append_footprint(self, footprint):
        """Append a footprint

        Args:
            footprint (Footprint): footprint
        """
        assert isinstance(footprint, type(Footprint()))
        self._response_internal["media_annotation"]["codes"].append(footprint)

    def append_track(self, video_ann):
        """Append a track to tracks_summary

        Args:
            video_ann (VideoAnn): track
        """
        assert isinstance(video_ann, type(VideoAnn()))
        self._response_internal["media_annotation"]["tracks_summary"].append(video_ann)

    def append_media_summary(self, video_ann):
        """Append a media_summary to media_summary section

        Args:
            video_ann (VideoAnn): media_summary
        """
        assert isinstance(video_ann, type(VideoAnn()))
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
        """Construct from a request. 
        
        Request object has a field for "prev_response", if not None, 
        convert prev_response into a Response
        
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
        """
        log.debug("Constructing Response from Request")
        if self._request.prev_response:
            log.debug("Loading from prev_response")
            prev_response_dict = None
            if self._request.bin_encoding is True:
                log.debug("bin_encoding is True")
                io = AvroIO()
                if isinstance(self._request.prev_response, str):
                    log.debug("prev_response is base64 encoded binary")
                    prev_response_dict = io.decode(
                        self._request.prev_response, use_base64=True, binary_flag=True
                    )
                else:
                    log.debug("prev_response is in binary")
                    prev_response_dict = io.decode(
                        self._request.prev_response, use_base64=False, binary_flag=True
                    )
            else:
                assert isinstance(self._request.prev_response, str)
                log.debug("prev_response is a JSON str")
                prev_response_dict = json.loads(self._request.prev_response)
            if prev_response_dict is None:
                raise ValueError("error: prev_response_dict is None")
            try:
                self._response_internal = ResponseInternal(**prev_response_dict)
            except Exception as e:
                log.error("error unpacking prev_response_dict")
                log.error(traceback.format_exc())
        elif self._request.prev_response_url:
            log.debug("Loading from prev_response_url")
            raise NotImplementedError()
        else:
            log.debug("No prev_response, constructing empty response_internal")
            self._response_internal = ResponseInternal()
        self.url = self._request.url
