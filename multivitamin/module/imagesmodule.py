import sys
import json
from abc import abstractmethod
import traceback
from collections.abc import Iterable
import pandas as pd
import glog as log

from multivitamin.module import Module, Codes
from multivitamin.module.utils import pandas_query_matches_props, batch_generator
from multivitamin.media import MediaRetriever


MAX_PROBLEMATIC_FRAMES = 10
BATCH_SIZE = 1


class ImagesModule(Module):
    def __init__(
        self,
        server_name,
        version,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
        batch_size=BATCH_SIZE,
    ):
        super().__init__(
            server_name=server_name,
            version=version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,
        )
        self.batch_size = batch_size
        log.debug(f"Creating ImagesModule with batch_size: {batch_size}")

    def process(self, response):
        """Process the message, calls process_images(batch, tstamps, contours=None)
           which is implemented by the child module

        Returns:
            Response: response object
        """
        log.debug("Processing message")
        super().process(response)

        try:
            log.info(f"Loading media from url: {self.response.request.url}")
            self.media = MediaRetriever(self.response.request.url)
            self.frames_iterator = self.media.get_frames_iterator(
                self.response.request.sample_rate
            )
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())
            self.code = Codes.ERROR_LOADING_MEDIA
            return self.update_and_return_response()

        self._update_w_h_in_response()

        if self.prev_pois and not self.response.has_frame_anns():
            log.warning("NO_PREV_REGIONS_OF_INTEREST, returning...")
            self.code = Codes.NO_PREV_REGIONS_OF_INTEREST
            return self.update_and_return_response()

        num_problematic_frames = 0
        for image_batch, tstamp_batch, prev_region_batch in batch_generator(
            self.preprocess_input(), self.batch_size
        ):
            if image_batch is None or tstamp_batch is None:
                continue
            try:
                self.process_images(image_batch, tstamp_batch, prev_region_batch)
            except ValueError as e:
                num_problematic_frames += 1
                log.warning("Problem processing frames")
                if num_problematic_frames >= MAX_PROBLEMATIC_FRAMES:
                    log.error(e)
                    self.code = Codes.ERROR_PROCESSING
                    return self.update_and_return_response()
        log.debug("Finished processing.")

        if self.prev_pois and self.prev_regions_of_interest_count == 0:
            log.warning("NO_PREV_REGIONS_OF_INTEREST, returning...")
            self.code = Codes.NO_PREV_REGIONS_OF_INTEREST
        return self.update_and_return_response()

    def preprocess_input(self):
        """Parses request for data

        Yields:
            frame: An image a time tstamp of a video or image
            tstamp: The timestamp associated with the frame
            region: The matching region dict
        """
        for i, (frame, tstamp) in enumerate(self.frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue

            if tstamp is None:
                log.warning("Invalid tstamp")
                continue

            self.tstamps_processed.append(tstamp)
            log.debug(f"tstamp: {tstamp}")
            if i % 100 == 0:
                log.info(f"tstamp: {tstamp}")

            if not self.prev_pois:
                yield frame, tstamp, None
            else:
                log.debug("Processing with previous response")
                log.debug(f"Querying on self.prev_pois: {self.prev_pois}")
                regions_that_match_props = []
                regions_at_tstamp = self.response.get_regions_from_tstamp(tstamp)
                log.debug(f"Finding regions at tstamp: {tstamp}")
                if regions_at_tstamp is not None:
                    log.debug(f"len(regions_at_tstamp): {len(regions_at_tstamp)}")
                    for i_region in regions_at_tstamp:
                        if self._region_contains_props(i_region):
                            log.debug(f"region: {i_region} contains props of interest")
                            regions_that_match_props.append(i_region)
                            self.prev_regions_of_interest_count += 1

                for region in regions_that_match_props:
                    yield frame, tstamp, region

    @abstractmethod
    def process_images(self, image_batch, tstamp_batch, prev_region_batch=None):
        """Abstract method to be implemented by child module"""
        pass

    def _update_w_h_in_response(self):
        (width, height) = self.media.get_w_h()
        log.debug(f"Setting in response w: {width} h: {height}")
        self.response.width = width
        self.response.height = height

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
