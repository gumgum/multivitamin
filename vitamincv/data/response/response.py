import os
import sys
import glog as log
import pprint
import tempfile
import json
import pprint
import pkg_resources
import numpy as np
import traceback
from collections import defaultdict
from datetime import datetime

from vitamincv.data.response.data import create_response


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

    def append_region(self, t, region):
        self._dictionary.get("media_annotation").get("frames_annotation")[t].append(region)

    def append_regions(self, t, regions):
        self._dictionary.get("media_annotation").get("frames_annotation")[t].extend(regions)

    def has_frame_anns(self):
        return len(self.dictionary.get("media_annotation").get("frames_annotation")) > 0

    def append_footprint(self, footprint):
        self._dictionary["media_annotation"]["codes"].append(footprint)

    def append_track(self, track):
        self._dictionary["media_annotation"]["tracks_summary"].append(track)

    def append_annotation_tasks(self, annotation_tasks):
        self._dictionary["media_annotation"]["annotation_tasks"].extend(annotation_tasks)

    def append_annotation_task(self, annotation_task):
        self._dictionary["media_annotation"]["annotation_tasks"].append(annotation_task)

    def sort_image_anns_by_timestamp(self):
        tmp = self._dictionary["media_annotation"]["frames_annotation"]
        self._dictionary["media_annotation"]["frames_annotation"] = sorted(tmp, key=lambda k: k["t"])

    def sort_tracks_summary_by_timestamp(self):
        tmp = self._dictionary["media_annotation"]["tracks_summary"]
        self._dictionary["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k["t1"])

    def compute_video_desc(self):
        raise NotImplementedError()

    @property
    def url(self):
        return self._dictionary["media_annotation"]["url"]

    @url.setter
    def url(self, url):
        assert isinstance(url, str)
        self._dictionary["media_annotation"]["url"] = url

    @property
    def url_original(self):
        return self._dictionary["media_annotation"]["url_original"]

    @url_original.setter
    def url_original(self, url):
        assert isinstance(url, str)
        self._dictionary["media_annotation"]["url_original"] = url

    @property
    def dims(self):
        return (self.width, self.height)

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

    @footprints.setter
    def footprints(self, fps):
        assert isinstance(fps, list)
        self._dictionary["media_annotation"]["codes"] = fps

    @property
    def width(self):
        return self._dictionary["media_annotation"]["w"]

    @width.setter
    def width(self, w):
        assert(isinstance(w, int))
        self._dictionary["media_annotation"]["w"]

    @property
    def height(self):
        return self._dictionary["media_annotation"]["h"]

    @height.setter
    def height(self, h):
        assert(isinstance(h, int))
        self._dictionary["media_annotation"]["h"]

    @property
    def timestamps(self):
        return sorted(self._dictionary["media_annotation"]["frames_annotation"].keys())

    @property
    def tracks(self):
        return self._dictionary["media_annotation"]["tracks_summary"]
