import sys
import json
from abc import abstractmethod
import traceback
from collections.abc import Iterable
import pandas as pd
import _thread
import glog as log

from multivitamin.module import Module, Codes
from multivitamin.module.utils import pandas_query_matches_props, batch_generator
from multivitamin.media import MediaRetriever


MAX_PROBLEMATIC_FRAMES = 10


class ImagesModule(Module):
    def __init__(
        self,
        server_name,
        version,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
        batch_size=1,        
        to_be_processed_buffer_size=1,
        parallel_downloading=False
    ):
        super().__init__(
            server_name=server_name,
            version=version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,           
            to_be_processed_buffer_size=to_be_processed_buffer_size
        )
        self.batch_size = batch_size
        self.parallel_downloading=parallel_downloading
        log.debug(f"Creating ImagesModule with batch_size: {batch_size}")

    @staticmethod
    def _fetch_media(response):
        """Fetches the media from response.request.url
           Careful, you must keep this method threadsafe
        """
        try:
            log.debug("preparing response")
            log.info(f"Loading media from url: {response.request.url}")
            response.media = MediaRetriever(response.request.url)
            response.frames_iterator = response.media.get_frames_iterator(
                response.request.sample_rate
            )
            ImagesModule._update_w_h_in_response(response=response)
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())
            response.code = Codes.ERROR_LOADING_MEDIA
            response.set_as_processed()#error downloading the image

        response.set_as_ready_to_be_processed()
        return

    def process(self, responses):
        """Process the message, calls process_images(batch, tstamps, contours=None)
           which is implemented by the child module

        Returns:
            list[Response]: responses objects
        """
        log.debug("Processing messages")
        super().process(responses)
        for r in self.responses_to_be_processed:
            if self.parallel_downloading:
                r.enablefsm()
            if r.is_to_be_processed():
                if r.set_as_preparing_to_be_processed()==False:
                    continue#if it was already set, we must continue
                if self.parallel_downloading:
                    _thread.start_new_thread(_fetch_media,(r))
                else:
                    self._fetch_media(r)
                
        for r in self.responses_to_be_processed:
            if r.is_ready_to_be_processed()==False:
                continue                
            if r.set_as_being_processed()==False:
                continue#if it was already set, we must continue
            if self.prev_pois and not r.has_frame_anns():
                    log.warning("NO_PREV_REGIONS_OF_INTEREST")
                    r.code = Codes.NO_PREV_REGIONS_OF_INTEREST
                    r.set_as_processed()#error, no previous regions of interest
                    continue
            if r.code == Codes.SUCCESS:
                num_problematic_frames = 0
                for image_batch, tstamp_batch, prev_region_batch, responses_batch  in batch_generator(
                    self.preprocess_input(response=r), batch_size=self.batch_size
                ):
                    if image_batch is None or tstamp_batch is None:
                        continue
                    try:
                        self.process_images(image_batch, tstamp_batch,prev_region_batch,responses_batch)
                    except ValueError as e:
                        num_problematic_frames += 1
                        log.warning("Problem processing frames")
                        if num_problematic_frames >= MAX_PROBLEMATIC_FRAMES:
                            log.error(e)
                            self.code = Codes.ERROR_PROCESSING
                            self.set_as_processed(r)
                            return self.update_and_return_response()
                r.set_as_processed()
        log.debug("Finished processing.")
        
        if self.prev_pois and self.prev_regions_of_interest_count == 0:
            log.warning("NO_PREV_REGIONS_OF_INTEREST, returning...")
            self.code = Codes.NO_PREV_REGIONS_OF_INTEREST
        return self.update_and_return_responses()

    def preprocess_input(self,response):
        """Parses request for data

        Yields:
            frame: An image a time tstamp of a video or image
            tstamp: The timestamp associated with the frame
            region: The matching region dict
            response: the actual response
        """
        for i, (frame, tstamp) in enumerate(response.frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue

            if tstamp is None:
                log.warning("Invalid tstamp")
                continue

            response.tstamps_processed.append(tstamp)
            log.debug(f"tstamp: {tstamp}")
            if i % 100 == 0:
                log.info(f"tstamp: {tstamp}")

            if not self.prev_pois:
                yield frame, tstamp, None, response
            else:
                log.debug("Processing with previous response")
                log.debug(f"Querying on self.prev_pois: {self.prev_pois}")
                regions_that_match_props = []
                regions_at_tstamp = response.get_regions_from_tstamp(tstamp)
                log.debug(f"Finding regions at tstamp: {tstamp}")
                if regions_at_tstamp is not None:
                    log.debug(f"len(regions_at_tstamp): {len(regions_at_tstamp)}")
                    for i_region in regions_at_tstamp:
                        if self._region_contains_props(i_region):
                            log.debug(f"region: {i_region} contains props of interest")
                            regions_that_match_props.append(i_region)
                            self.prev_regions_of_interest_count += 1

                for region in regions_that_match_props:
                    yield frame, tstamp, region, response

    @abstractmethod
    def process_images(self, image_batch, tstamp_batch, responses, prev_region_batch=None):
        """Abstract method to be implemented by child module"""
        pass

    @staticmethod
    def _update_w_h_in_response(response):
        (width, height) = response.media.get_w_h()
        log.debug(f"Setting in response w: {width} h: {height}")
        response.width = width
        response.height = height

    def _region_contains_props(self, region):
        """ Boolean to check if a region's props matches the defined
            previous properties of interest
        
        Args:
            props (list): list of properties for a region
        Returns:
            bool: if props match prev_pois query
        """
        props = region.get("props")
        if props is None:
            return False
        return pandas_query_matches_props(self.prev_pois_bool_exp, pd.DataFrame(props))
