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
from vitamincv.data.response.data import *


class Response:
    def __init__(self, dictionary=None, request=None):
        """Wrapper with utilities around a single response document"""
        log.info("Constructing response")
        self.dictionary = dictionary
        self.request = request
        self.tstamp_map = None
        self.url = request.url

    @property
    def dictionary(self):
        return self._dictionary

    @dictionary.setter
    def dictionary(self, dictionary):
        if dictionary is None:
            log.info("Response.dictionary is none, creating empty response")
            self._dictionary = create_response()
        else:
            log.info("Response.dictionary is NOT none, creating from previous response")
            self._dictionary = dictionary

    @property
    def frame_anns(self):
        return self._dictionary.get("media_annotation").get("frames_annotation")

    def to_bytes(self):
        raise NotImplementedError()

    def append_region(self, t, region):
        self._dictionary.get("media_annotation").get("frames_annotation")[t].append(region)

    def append_regions(self, t, regions):
        frame_anns = self._dictionary.get("media_annotation").get("frames_annotation")[t]
        if len(frame_anns) == 0:
            log.info(f"frame anns len == 0, creating new frame ann w/ len(regions): {len(regions)}")
            self._dictionary.get("media_annotation").get("frames_annotation")[t].extend(regions)
        else:
            log.info(
                f"frame anns[t] len == {len(frame_anns)}, appending regions frame ann w/ len(regions): {len(regions)}"
            )
            self._dictionary.get("media_annotation").get("frames_annotation")[t].extend(regions)
        log.info(f"len(frame_anns[t]): {len(frame_anns)}")
        frame_anns = self._dictionary.get("media_annotation").get("frames_annotation")[t]
        log.info(f"len(frame_anns[t]): {len(frame_anns)}")

    def has_frame_anns(self):
        return len(self.dictionary.get("media_annotation").get("frames_annotation")) > 0

    def append_footprint(self, footprint):
        self._dictionary["media_annotation"]["codes"].append(footprint)

    def append_video_ann(self, track):
        self._dictionary["media_annotation"]["tracks_summary"].append(track)

    def append_annotation_tasks(self, annotation_tasks):
        for at in annotation_tasks:
            self.append_annotation_task(at)

    def append_annotation_task(self, annotation_task):
        self._dictionary["media_annotation"]["annotation_tasks"].append(annotation_task)

    def sort_image_anns_by_timestamp(self):
        tmp = self._dictionary["media_annotation"]["frames_annotation"]
        self._dictionary["media_annotation"]["frames_annotation"] = sorted(tmp, key=lambda k: k["t"])

    def sort_tracks_summary_by_timestamp(self):
        tmp = self._dictionary["media_annotation"]["tracks_summary"]
        self._dictionary["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k["t1"])

    def compute_video_desc(self):
        pass

    def set_media_ann(self, media_ann):
        assert isinstance(media_ann, dict)
        self._dictionary["media_annotation"] = media_ann

    @property
    def url(self):
        return self._dictionary["media_annotation"]["url"]

    @url.setter
    def url(self, url):
        assert isinstance(url, str)
        self._dictionary["media_annotation"]["url"] = url

    def set_url_original(self, url):
        assert isinstance(url, str)
        self._dictionary["media_annotation"]["url_original"] = url

    def set_dims(self, w, h):
        assert isinstance(w, int)
        assert isinstance(h, int)
        self._dictionary["media_annotation"]["w"] = w
        self._dictionary["media_annotation"]["h"] = h

    def set_footprints(self, codes):
        assert isinstance(codes, list)
        self._dictionary["media_annotation"]["codes"] = codes

    """Getters"""

    @property
    def media_summaries(self):
        """Get full media summary

        Returns:
            list of media_summary objects
        """
        return self._dictionary["media_annotation"]["media_summary"]

    @property
    def footprints(self):
        """Get all footprints

        Returns:
            list of footprints
        """
        return self._dictionary["media_annotation"]["codes"]

    @property
    def dims(self):
        return (self.width, self.height)

    @property
    def width(self):
        return self._dictionary["media_annotation"]["w"]

    @property
    def height(self):
        return self._dictionary["media_annotation"]["h"]

    @property
    def timestamps(self):
        return sorted(self._dictionary["media_annotation"]["frames_annotation"].keys())

    def get_timestamps_from_footprints(self, server=None):
        tstamps = []
        for c in self._dictionary["media_annotation"]["codes"]:
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

    @property
    def tracks(self):
        return self._dictionary["media_annotation"]["tracks_summary"]
