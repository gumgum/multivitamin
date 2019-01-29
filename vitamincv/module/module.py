import json
import glog as log
import datetime
import os
import traceback
from abc import ABC, abstractmethod

from vitamincv.media.media import MediaRetriever
from vitamincv.data.request import Request
from vitamincv.data.data import ModuleData
from vitamincv.module.utils import p0p1_from_bbox_contour, list_contains_only_none

class Module(ABC):
    def __init__(self, server_name, version, prop_type=None,
                 prop_id_map=None, module_id_map=None):
        """CVModule is an abstract class that defines the interface for 
        two abstract children: 

        ImageModule, PropertiesModule

        Handles processing of request and previous module_data
        """
        self.name = server_name
        self.version = version
        self.prop_type = prop_type
        self.prop_id_map = prop_id_map
        self.module_id_map = module_id_map
        self.module_data = ModuleData()

    def set_prev_props_of_interest(self, pois):
        self.prev_pois = pois
    
    def get_prev_props_of_interest(self):
        return self.prev_pois

    @abstractmethod
    def process(self, request, prev_module_data=None):
        assert isinstance(request, Request)
        self.request = request
        self.prev_module_data = prev_module_data

class PropertiesModule(Module):
    def process(self, request, prev_module_data=None):
        super().process(request, prev_module_data)
        self.process_properties()
        return self.get_module_data()

    @abstractmethod
    def process_properties():
        """Abstract method to be implemented to the child PropertiesModule, which appends to

        self.segments
        """
        pass

class ImageModule(Module):
    def process(self, request, prev_module_data=None):
        super().process(request, prev_module_data)
        self.med_ret = MediaRetriever(request.url)
        self.frames_iterator = self.med_ret.get_frames_iterator(request.sample_rate)
        for i, (frame, tstamp) in enumerate(self.frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue
            if tstamp is None:
                log.warning("Invalid tstamp")
                continue
            log.info(f"tstamp: {tstamp}")

            try:
                if prev_module_data:
                    prev_dets = prev_module_data.detections.tstamp_map.get(tstamp, None)
                    ## todo, look for POIs
                    if not prev_dets or list_contains_only_none(prev_dets):
                        log.info("prev_dets is empty")
                        continue
                    log.debug(f"Processing with {len(prev_dets)} previous detections")
                    images = self.crop_image_from_(frame, prev_dets)
                    assert(len(images)==len(prev_dets))
                    for image, det in zip(images, prev_dets):
                        self.process_image(image, tstamp, det)
                else:
                    self.process_image(frame, tstamp)
            except:
                log.error(traceback.print_exc())
        return self.module_data
    
    def crop_image(self, image, prev_detections=None):
        log.info("Cropping images with previous detections")
        if prev_detections is None or list_contains_only_none(prev_detections):
            return image
        
        images = []
        for det in prev_detections:
            if det.get("contour") is None:
                log.warning("Contour is None")
                continue
            h, w = image.shape
            (x0, y0), (x1, y1) = p0p1_from_bbox_contour(det["contour"], w, h)
            images.append(image[y0:y1, x0:x1])
        return images
        
    @abstractmethod
    def process_image(self, image, tstamp, prev_det=None):
        """Abstract method to be implemented by the child ImageModule, which appends to

        self.detections
        """
        pass