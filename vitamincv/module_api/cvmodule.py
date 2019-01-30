from abc import ABC, abstractmethod
import json
import glog as log
import datetime
import os
import traceback
from collections.abc import Iterable

from vitamincv.comm_apis.request_api import RequestAPI
from vitamincv.avro_api.avro_api import AvroAPI, AvroIO
from vitamincv.avro_api.utils import get_current_time
from vitamincv.avro_api.cv_schema_factory import *
MAX_PROBLEMATIC_FRAMES=10

class CVModule(ABC):
    @abstractmethod
    def __init__(self, server_name, version, prop_type=None, prop_id_map=None, module_id_map=None,process_properties_flag=False):
        """Abstract base class for CVModules

        Args:
            server_name (str): name of server/module
            version (str): version of module
            idmap (str): filepath to idmap
        """
        log.info('Constructing cvmodule')
        self.name = server_name
        self.version = version
        self.batch_size = 1 #This is to be defined depending on the architecture of the machine and model, and in the resolution of the images
        self.prop_type=prop_type
        self.prop_id_map=prop_id_map
        self.module_id_map=module_id_map

        self.min_conf_filter = {}

        #To be overwritten, if needed, by the child
        self.process_properties_flag = process_properties_flag
        self.prev_pois = [] #this are the properties of interest that define the regions of interest, the ones that we'll want to analyze.
        self.detections_of_interest=[]#A list with the detections of interest, the ones that we'll want to analyze.
        self.code='SUCCESS'
        self.num_problematic_frames=0
        self.detections=[]

    def set_prev_pois(self, prev_pois):
        log.info("Setting prev_pois: " + str(prev_pois))
        self.prev_pois=prev_pois

    def set_message(self, message):
        if isinstance(message, RequestAPI):
            log.info("message is a Request_API")
            self.request_api=message
        elif isinstance(message, list):
            for l in message:
                if not isinstance(l, RequestAPI):
                    raise ValueError("Message is not of type RequestAPI")
            if len(message) <= 0:
                raise ValueError("Empty list")
            log.info("message is a list of Request_API, setting to 0th index")
            self.request_api=message[0]
        elif isinstance(message, str):
            log.info("message is a string, creating RequestAPI")
            self.request_api=RequestAPI(message)
        elif  isinstance(message, dict):
            log.info("message is a dictionary, creating RequestAPI")
            self.request_api=RequestAPI(message)
        else:
            raise TypeError("Unsupported type of message: {}".format(type(message)))

        self.avro_api=self.request_api.get_avro_api()
        self.media_api=self.request_api.get_media_api()
        self.draw_detections=self.request_api.get("draw_detections")
        self.sample_rate = self.request_api.sample_rate
        if self.request_api.is_in("prev_pois"):#if not, the child will have defined them
            self.prev_pois=self.request_api.get("prev_pois")
        self.code='SUCCESS'

    def get_request_api(self):
        return self.request_api

    def get_avro_api(self):
        return self.avro_api

    def findrois(self):
        log.info("Looking for rois.")
        if len(self.prev_pois)==0:
            log.debug("No previous properties of interest to be searched.")
            self.detections_of_interest=[]
            return
        if len(self.prev_pois): #properties of interested were defined in the child, or by means of the request.
            log.debug("There are previous properties of interest to be searched.")
            #we retrieve the regions of interest from previous response,avro_api.get_regions_from_prop will need to also return tstamps of the regions
            try:
                self.detections_of_interest=self.avro_api.get_detections_from_props(self.prev_pois)
            except Exception as e:
                log.warning(e)
            log.debug("len(self.detections_of_interest): " + str(len(self.detections_of_interest)))
            log.debug("Creating self.detections_t_map.")
            self.detections_t_map=self.avro_api.create_detections_tstamp_map(self.detections_of_interest)

    def batch_generator(self, iterator):
        """Take an iterator, convert it to a chunking generator


        Args:
            iterator: Any iterable object where each element is a list or a tuple of length N

        Yields:
            list: A list of N batches of size `self.batch_size`. The last
                    batch may be smaller than the others
        """
        batch = []
        for iteration in iterator:
            batch.append(iteration)
            if len(batch) >= self.batch_size:
                yield zip(*batch)
                batch = []
        if len(batch) > 0:
            yield zip(*batch)

    def preprocess_message(self):
        """Parses HTTP message for data

        Yields:
            frame: An image a time tstamp of a video or image
            tstamp: The timestamp associated with the frame
            det: The matching detection object
        """
        log.info("Processing frames")
        frames_iterator=[]
        try:
            frames_iterator = self.media_api.get_frames_iterator(self.request_api.sample_rate)
        except ValueError as e:
            log.error(e)
            self.code = "ERROR_NO_IMAGES_LOADED"
            return self.code

        images = []
        tstamps = []
        detections_of_interest = []
        for i, (frame, tstamp) in enumerate(frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue

            if tstamp is None:
                log.warning("Invalid tstamp")
                continue

            log.info('tstamp: ' + str(tstamp))
            dets = [None]
            if len(self.prev_pois) > 0: #We are expected to focus on previous detections
                if tstamp in self.detections_t_map:
                    dets = self.detections_t_map[tstamp]

                if len(dets) == 0:
                    log.debug("No detections for tstamp " + str(tstamp))
                    continue

            for det in dets:
                yield frame, tstamp, det

    def process(self, message):
        """Process the message, calls process_images(batch, tstamps, contours=None)

        Returns:
            str code
        """
        log.info("Setting message")
        self.set_message(message)
        log.info("Processing message")

        #if the module is a "properties's processor, we call and return the correspondent child method.
        if self.process_properties_flag:
            log.info("Processing properties")
            try:
                self.process_properties()
            except ValueError as e:
                log.error(e)
                log.error("Problems processing properties")
                self.code = e
            return self.code

        self.num_problematic_frames = 0
        self.findrois() #This will update self.detections_of_interest

        self.detections = []
        for image_batch, tstamp_batch, det_batch in self.batch_generator(self.preprocess_message()):
            if self.num_problematic_frames >= MAX_PROBLEMATIC_FRAMES:
                log.error("Too Many Problematic Iterations")
                log.error("Returning with error code: "+str(self.code))
                return self.code

            try:
                image_batch = self.preprocess_images(image_batch, det_batch)
            except Exception as e:
                log.error("Image Preprocessing Failed")
                log.error(e)
                self.code = e
                self.num_problematic_frames += 1
                continue

            try:
                prediction_batch_raw = self.process_images(image_batch)
            except Exception as e:
                log.error("Image Inferencing Failed")
                log.error(e)
                self.num_problematic_frames += 1
                continue

            try:
                prediction_batch = self.postprocess_predictions(prediction_batch_raw)
            except Exception as e:
                log.error("Inference Postprocssing Failed")
                log.error(e)
                self.code = e
                self.num_problematic_frames += 1
                continue

            try:
                for predictions, tstamp, prev_det in zip(prediction_batch, tstamp_batch, det_batch):
                    iterable = self.convert_to_detection(predictions=predictions,
                                             tstamp=tstamp,
                                             previous_detection=prev_det)
                    if not isinstance(iterable, Iterable) or isinstance(iterable, dict):
                        iterable = [iterable]

                    for new_det in iterable:
                        self.detections.append(new_det)
            except Exception as e:
                log.error("Appending Detections Failed")
                log.error(e)
                self.code = e
                self.num_problematic_frames += 1
                continue

        log.debug("Finished processing.")
        return self.code

    def preprocess_images(self, images, contours = None):
        return images

    def process_images(self, images, tstamps, detections_of_interest=None):
        """Abstract method to be implemented by child module"""
        pass

    def process_properties(self):
        """Abstract method to be implemented by child module"""
        pass

    def postprocess_predictions(self, predictions):
        return predictions

    def convert_to_detection(self, predictions, tstamp=None, previous_detection=None):
        pass

    def update_response(self):
        """Iterate over self.detections that has been populated by the child,
           and populate response document
        """
        log.info("Adding footprint.")
        date = get_current_time()
        n_footprints=len(self.avro_api.get_footprints())
        footprint_id=date+str(n_footprints+1)
        fp=create_footprint(code=self.code, ver=self.version, company="gumgum", labels=None, server_track="",
                     server=self.name, date=date, annotator="",
                     tstamps=None, id=footprint_id)
        self.avro_api.append_footprint(fp)
        log.info('self.code: ' + str(self.code))
        if self.code!='SUCCESS':
            log.error('The processing was not succesful')
            return
        self.avro_api.set_url(self.request_api.get_url())
        self.avro_api.set_url_original(self.request_api.get_url())
        self.avro_api.set_dims(*self.request_api.media_api.get_w_h())

        if self.process_properties_flag==False:
            module_id=0
            if self.module_id_map:
                module_id=self.module_id_map.get(self.name)
                if module_id is None:
                    log.warning('module '+ self.name +' is not in module_id_map.')
            log.info("len(self.detections): " + str(len(self.detections)))
            for det in self.detections:
                value = det.get("value")
                if value is None:
                    log.warning("det['value'] should have a value at this point.")
                det['server'] = self.name
                det['module_id'] = module_id
                det['ver'] = self.version
                det['footprint_id'] = footprint_id
                try:
                    self.avro_api.append_detection(det,self.prop_id_map)
                except:
                    log.error("Problem appending detection.")
                    log.error(traceback.print_exc())
            self.detections=[]
        else:
            log.info("Not appending any detection, this was a property processor module")
        log.debug("We reset the queriers to None. If a caller performs a query they need to be rebuilt with the new detections and segments.")
        self.avro_api.reset_queriers()
