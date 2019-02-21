import sys
import json
from abc import abstractmethod
import traceback
from collections.abc import Iterable
import pandas as pd
import glog as log

from vitamincv.module import Module, Codes
from vitamincv.media import MediaRetriever



MAX_PROBLEMATIC_FRAMES = 10
BATCH_SIZE = 2


class ImagesModule(Module):
    def __init__(
        self, server_name, version, prop_type=None, prop_id_map=None, module_id_map=None, batch_size=BATCH_SIZE
    ):
        super().__init__(
            server_name=server_name,
            version=version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,
        )
        self.batch_size = batch_size
        log.info(f"Creating ImagesModule with batch_size: {batch_size}")

    def process(self, request, response):
        """Process the message, calls process_images(batch, tstamps, contours=None)

        Returns:
            str code
        """
        log.info("Processing message")
        super().process(request, response)

        frames_iterator=[]
        try:
            self.media = MediaRetriever(self.request.url)
            self.frames_iterator = self.media.get_frames_iterator(self.request.sample_rate)
        except Exception as e:
            self.code = Codes.ERROR_LOADING_MEDIA
            return self.update_and_return_response()

        if self.prev_pois and not self.response.has_frame_anns():
            self.code = Codes.NO_PREV_REGIONS_OF_INTEREST
            return self.update_and_return_response()

        num_problematic_frames=0
        for image_batch, tstamp_batch, prev_region_batch in self.batch_generator(self.preprocess_input()):
            try:
                self.process_images(image_batch, tstamp_batch, prev_region_batch)
            except ValueError as e:
                num_problematic_frames+=1
                log.warning('Problem processing frames')
                if num_problematic_frames>=MAX_PROBLEMATIC_FRAMES:
                    log.error(e)
                    self.code = Codes.ERROR_PROCESSING
                    return self.update_and_return_response()
        log.info("Finished processing.")

        if self.prev_pois and self.prev_regions_of_interest_count == 0:
            self.code = Codes.NO_PREV_REGIONS_OF_INTEREST
        return self.update_and_return_response()

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

            log.info(f"tstamp: {tstamp}")

            regions = []
            if self.prev_pois:
                log.info("Processing with previous response")
                log.info(f"Querying on self.prev_pois: {self.prev_pois}")

                all_tstamp_regions = self.response.frame_anns.get(tstamp)
                if all_tstamp_regions is not None:
                    for tregion in all_tstamp_regions:
                        if self._region_matches_prev_pois(tregion):
                            regions.append(tregion)
                            self.prev_regions_of_interest_count += 1

            if len(regions) == 0:
                yield frame, tstamp, None
            
            for region in regions:
                yield frame, tstamp, region

    @abstractmethod
    def process_images(self, image_batch, tstamp_batch, prev_region_batch=None):
        """Abstract method to be implemented by child module"""
        pass

    def _region_matches_prev_pois(self, region):
        """Returns true if region has properties that match prev_pois, 
        else returns false
        """
        log.debug(f"Prev pois query: {self.pd_query_prev_pois}")
        log.debug(f"against region: {region}")
        props_pd = pd.DataFrame(region.get("props"))
        queried_pois = props_pd.query(self.pd_query_prev_pois)
        log.debug(f"matches? : {not queried_pois.empty}")
        return not queried_pois.empty

