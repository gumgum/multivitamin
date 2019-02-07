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

from datetime import datetime

from vitamincv.data.utils import points_equal, times_equal, create_region_id, get_current_time
from vitamincv.data import MediaData, create_detection, create_segment, create_bbox_contour_from_points, create_point
from vitamincv.data.avro_response import config
from vitamincv.data.avro_response.cv_schema_factory import (
    create_footprint,
    create_response,
    create_region,
    create_image_ann,
    create_video_ann,
    create_prop
)
from vitamincv.data.avro_response.avro_io import AvroIO
from vitamincv.data.response_interface import Response

import importlib.util

if importlib.util.find_spec("cupy"):
    import cupy as cp


class AvroResponse(Response):
    """Wrapper with utilities around a single response document"""

    def set_response(self, response):
        if not response:
            response = create_response()
        if not isinstance(response, dict):
            raise ValueError(dict)
        self.response = response

    def load_mediadata(self, media_data):
        """Conver ModuleData to response"""

        log.info("Loading mediadata into response")
        date = get_current_time()
        num_footprints = len(self.get_footprints())
        footprint_id = date + str(num_footprints + 1)
        footprint = create_footprint(
            code=media_data.code,
            ver=media_data.meta.get("ver"),
            company="gumgum",
            labels=None,
            server_track="",
            server=media_data.meta.get("name"),
            date=date,
            annotator="",
            tstamps=None,
            id=footprint_id,
        )
        self.append_footprint(footprint)
        self.set_url(media_data.meta.get("url"))
        self.set_url_original(media_data.meta.get("url"))
        if self.get_dims() == (0, 0):
            self.set_dims(*media_data.meta.get("dims"))

        for det in media_data.detections:
            log.debug(f"Appending det: {det} to frame_anns")
            self.append_detection(det)

        for seg in media_data.segments:
            log.debug(f"Appending seg: {seg} to tracks_summary")
            self.append_track_to_tracks_summary(seg)
        self.sort_tracks_summary_by_timestamp()

    def to_mediadata(self, props_of_interest=None):
        """Convert response data to ModuleData type

        Args:
            properties_of_interest (dict): dictionary with properties of interest
        """
        log.info("Converting response to mediadata")

        md = MediaData()
        
        dets = self.get_detections_from_frame_anns()
        md.detections = dets
        if props_of_interest is not None:
            md.filter_dets_by_props_of_interest(props_of_interest)

        # TODO 
        # segs = self._get_segments_from_response(properties_of_interest)  
        # # if segs:
        # #     log.info(f"Found {len(segs)} segs")

        md.update_maps()
        return md

    def to_dict(self):
        return self.response

    def to_bytes(self):
        return None

    """Modifiers"""

    def append_footprint(self, code):
        self.response["media_annotation"]["codes"].append(code)

    def append_detection(self, detection, prop_id_map=None, t_eps=None):
        """TODO
        Use region_id map for this method, reevaluate all the branchings
        
        Append a detection struct (the middleman between avro schema and API user)
        """
        log.debug("Appending detection w/ tstamp: {}".format(detection["t"]))

        value = detection.get("value")
        property_id = detection.get("property_id")
        if value is None and property_id is None:
            log.error("detection with no value or property id.")
            return

        if value and property_id and prop_id_map:
            log.warning(
                "Both value and property_id are not None. But there is a property_id map. property_id will be overwritten"
            )
        if prop_id_map and value:
            property_id = prop_id_map.get(value, 0)

        if value is None:
            value = ""
        if property_id is None:
            property_id = 0

        region_id_query = detection["region_id"]
        if len(region_id_query) > 0:
            region = self.get_region_from_region_id(region_id_query)
            if region:  # If the region already exists
                # log.debug("The region " + region_id_query + "already existed")
                # log.debug ("Creating prop")
                prop = create_prop(
                    server=detection["server"],
                    module_id=detection["module_id"],
                    property_type=detection["property_type"],
                    property_id=property_id,
                    value=value,
                    confidence=float(detection["confidence"]),
                    value_verbose=detection["value_verbose"],
                    fraction=detection["fraction"],
                    footprint_id=detection["footprint_id"],
                    ver=detection["ver"],
                    company=detection["company"],
                )
                # log.debug("Appending prop")
                region["props"].append(prop)
                return
        log.debug("The region " + region_id_query + " did not previously exist.")
        if len(region_id_query) > 0:
            region_id = region_id_query
        else:
            region_id = create_region_id(detection["t"], detection["contour"])

        region = self.get_region_from_region_id(region_id)
        if region:  # If the region already exists
            log.debug("The region " + region_id_query + "already existed")
            log.debug("Creating prop")
            prop = create_prop(
                server=detection["server"],
                module_id=detection["module_id"],
                property_type=detection["property_type"],
                property_id=property_id,
                value=value,
                confidence=float(detection["confidence"]),
                value_verbose=detection["value_verbose"],
                fraction=detection["fraction"],
                footprint_id=detection["footprint_id"],
                ver=detection["ver"],
                company=detection["company"],
            )
            log.debug("Appending prop")
            region["props"].append(prop)
            return

        log.debug("The region " + region_id_query + " did not previously exist.")

        region = create_region(
            props=[
                create_prop(
                    server=detection["server"],
                    module_id=detection["module_id"],
                    property_type=detection["property_type"],
                    property_id=property_id,
                    value=value,
                    confidence=float(detection["confidence"]),
                    value_verbose=detection["value_verbose"],
                    fraction=detection["fraction"],
                    footprint_id=detection["footprint_id"],
                    ver=detection["ver"],
                    company=detection["company"],
                )
            ],
            contour=detection["contour"],
            id=region_id,
        )

        # TODO: once we change frame_annotations into map, no need to check this
        tstamp_exists = False
        t2 = detection["t"]
        i = 0
        # https://stackoverflow.com/questions/21388026/find-closest-float-in-array-for-all-floats-in-another-array
        for i, image_ann in enumerate(list(self.response["media_annotation"]["frames_annotation"])):
            t1 = image_ann["t"]
            if times_equal(float(t1), float(t2), eps=t_eps):
                log.debug("Appending new region to existing frame annotation.")
                image_ann["regions"].append(region)
                self.response["media_annotation"]["frames_annotation"][i] = image_ann
                tstamp_exists = True
                break
            if t1 > t2 and not times_equal(
                float(t1), float(t2), eps=t_eps
            ):  # redundant I know. Just in case we remove the equality check from lines above
                break
        if not tstamp_exists:
            log.debug("Creating new frame annotation with tstamp: {}".format(detection["t"]))
            image_ann = create_image_ann(t=detection["t"], regions=[region])
            self.response["media_annotation"]["frames_annotation"].insert(i, image_ann)

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
        self.response["media_annotation"]["frames_annotation"].append(image_ann)

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

