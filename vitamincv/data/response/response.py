import os
import sys
import glog as log
import pprint
import tempfile
import json
import pprint
import pkg_resources
import numpy as np
import GPUtil
import traceback
from collections import defaultdict
from datetime import datetime

from vitamincv.data.response.utils import points_equal, times_equal, create_region_id, get_current_time
from vitamincv.data.response.io import AvroIO
from vitamincv.data.response.data import *

def load_response_from_request(request):
    """Methodfor loading a previous response from a request"""
    log.info("Loading a response")
    try:
        if not request.prev_response:
            log.info("No prev_response")
            return Response(request=request)
        
        if request.bin_encoding is True:
            log.info("bin_encoding is True")
            io = AvroIO()
            if isinstance(request.prev_response, str):
                log.info("prev_response is base64 encoded")
                bytes = io.decode(request.prev_response, use_base64=True, binary_flag=True)
            else:
                log.info("prev_response is in binary")
                bytes = io.decode(request.prev_response, use_base64=False, binary_flag=True)
            return Response(dictionary=io.decode(bytes), request=request)

        if isinstance(request.prev_response, dict):
            log.info("prev_response is a dict")
            return Response(dictionary=request.prev_response, request=request)

    except Exception as e:
        log.error(traceback.print_exc())
        log.error(e)
        log.error("Error loading previous response")
    log.info("Decoded prev_response")

class Response():
    def __init__(self, dictionary=None, request=None):
        """Wrapper with utilities around a single response document"""
        log.info("Constructing response")
        self.dictionary = dictionary
        self.request = request
        self.tstamp_map = None

    @property
    def dictionary(self):
        return self._dictionary
    
    @dictionary.setter
    def dictionary(self, dictionary):
        if dictionary is None:
            log.info("Dictionary is none, creating empty response")
            self._dictionary = create_response() 
        else:
            log.info("Dictionary is NOT none, creating from previous response")
            self._dictionary = dictionary
        self._create_tstamp_map()

    @dictionary.getter
    def dictionary(self):
        return self._dictionary

    def to_bytes(self):
        raise NotImplementedError()

    def has_frame_anns(self):
        return len(self.dictionary.get("media_annotation").get("frames_annotation")) > 0
    
    def update_maps(self):
        self._create_tstamp_map()

    def _create_tstamp_map(self):
        log.info("Creating tstamp map")
        self.tstamp_map = defaultdict(list)
        for frame_ann in self.dictionary.get("media_annotation").get("frames_annotation"):
            self.tstamp_map[frame_ann.get("t")] = frame_ann.get("regions")

    def append_footprint(self, code):
        self.response["media_annotation"]["codes"].append(code)

    def get_region_from_region_id(self, region_id):
        # 
        # OBSOLETE
        # 
        for image_ann in self.response["media_annotation"]["frames_annotation"]:
            for region in image_ann["regions"]:
                if region["id"] == region_id:
                    return region
        return None

    def append_image_anns(self, image_anns):
        for image_ann in image_anns:
            self.append_image_ann(image_ann)

    def append_image_ann(self, image_ann):
        self._dictionary["media_annotation"]["frames_annotation"].append(image_ann)

    def append_region_to_image_ann(self, region, tstamp):
        # Potentially change frames_annotations into a map for O(1) lookup in the future
        for image_ann in self.response["media_annotation"]["frames_annotation"]:
            if image_ann["t"] == tstamp:
                image_ann["regions"].append(region)

    def append_track_to_tracks_summary(self, track):
        # log.info(pprint.pformat(track))
        if "array" in self.response["media_annotation"]["tracks_summary"]:
            self.response["media_annotation"]["tracks_summary"]["array"].append(track)
        else:
            self.response["media_annotation"]["tracks_summary"].append(track)

    def append_annotation_tasks(self, annotation_tasks):
        for at in annotation_tasks:
            self.append_annotation_task(at)

    def append_annotation_task(self, annotation_task):
        self.response["media_annotation"]["annotation_tasks"].append(annotation_task)

    def sort_image_anns_by_timestamp(self):
        tmp = self.response["media_annotation"]["frames_annotation"]
        self.response["media_annotation"]["frames_annotation"] = sorted(tmp, key=lambda k: k["t"])

    def sort_tracks_summary_by_timestamp(self):
        tmp = self.response["media_annotation"]["tracks_summary"]
        self.response["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k["t2"])
        self.response["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k["t1"])

    def compute_video_desc(self):
        pass

    """Setters"""

    def set_media_ann(self, media_ann):
        assert isinstance(media_ann, dict)
        self._clear_maps()
        self.response["media_annotation"] = media_ann

    def set_url(self, url):
        assert isinstance(url, str)
        self.response["media_annotation"]["url"] = url

    def set_url_original(self, url):
        assert isinstance(url, str)
        self.response["media_annotation"]["url_original"] = url

    def set_dims(self, w, h):
        assert isinstance(w, int)
        assert isinstance(h, int)
        self.response["media_annotation"]["w"] = w
        self.response["media_annotation"]["h"] = h

    def set_footprints(self, codes):
        assert isinstance(codes, list)
        self.response["media_annotation"]["codes"] = codes

    """Getters"""

    def get_servers(self):
        """Get all servers from avro doc

        Returns:
            list (str): list of servers
        """
        servers = []
        for code in self.response["media_annotation"]["codes"]:
            servers.append(code["server"])
        return servers

    def get_codes(self):
        """Get all footprints from avro doc

        Returns:
            list (dict): list of footprints
        """
        return self.response["media_annotation"]["codes"]

    def get_image_anns(self):
        """Get all frame annotations

        Returns:
            list of image_ann objects
        """
        return self.response["media_annotation"]["frames_annotation"]

    def get_media_summaries(self):
        """Get full media summary

        Returns:
            list of media_summary objects
        """
        return self.response["media_annotation"]["media_summary"]

    def get_footprints(self):
        """Get all footprints

        Returns:
            list of footprints
        """
        return self.response["media_annotation"]["codes"]

    def get_url(self):
        return self.response["media_annotation"]["url"]

    def get_dims(self):
        return (self.get_width(), self.get_height())

    def get_width(self):
        return self.response["media_annotation"]["w"]

    def get_height(self):
        return self.response["media_annotation"]["h"]

    def get_response(self):
        return self.response

    def get_json(self, indent=None):
        return json.dumps(self.response, indent=indent)

    def get_timestamps(self):
        return sorted([x["t"] for x in self.response["media_annotation"]["frames_annotation"]])

    def get_timestamps_from_footprints(self, server=None):
        tstamps = []
        for c in self.response["media_annotation"]["codes"]:
            log.info(str(c))
            if not c["tstamps"]:
                continue
            if server:
                if c["server"] != server:
                    continue
            if not tstamps:
                log.info("Assigning timestamps: " + str(c["tstamps"]))
                tstamps = c["tstamps"]
            else:
                tstamps = list(set(tstamps) & set(c["tstamps"]))
        return tstamps

    def get_tracks(self):
        return self.response["media_annotation"]["tracks_summary"]

