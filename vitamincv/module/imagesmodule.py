from abc import abstractmethod
import traceback
from collections.abc import Iterable

import glog as log

from vitamincv.module import Module
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

    def process(self, request, prev_response=None):
        """Process the message, calls process_images(batch, tstamps, contours=None)

        Returns:
            str code
        """
        log.info("Processing message")
        super().process(request, prev_response)
        self._load_media()
        self.num_problematic_frames = 0
        for image_batch, tstamp_batch, prev_region_batch in self.batch_generator(self.preprocess_request()):
            if self.num_problematic_frames >= MAX_PROBLEMATIC_FRAMES:
                log.error("Too Many Problematic Iterations")
                log.error("Returning with error code: " + str(self.code))
                return self.code

            try:
                self.process_images(image_batch, tstamp_batch, prev_region_batch)
            except Exception as e:
                log.error(e)
                log.error(traceback.print_exc())
                self.code = e
                self.num_problematic_frames += 1
                continue

        log.debug("Finished processing.")
        return self.code

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

    def preprocess_request(self):
        """Parses request for data

        Yields:
            frame: An image a time tstamp of a video or image
            tstamp: The timestamp associated with the frame
            region: The matching region dict
        """
        frames_iterator = []
        try:
            frames_iterator = self.media.get_frames_iterator(self.request.sample_rate)
        except ValueError as e:
            log.error(traceback.print_exc())
            log.error(e)
            self.code = "ERROR_NO_IMAGES_LOADED"
            return self.code

        for i, (frame, tstamp) in enumerate(frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue

            if tstamp is None:
                log.warning("Invalid tstamp")
                continue

            if i % 100 == 0:
                log.info(f"tstamp: {tstamp}")

            regions = [None]
            if self.prev_response:
                log.info("Processing with previous response")
                regions = self.prev_response.tstamp_regions_map.get(tstamp, [None])
                log.info(f"Found {len(regions)} dets from previous media_data")

                if len(regions) == 0:
                    log.debug(f"No detections for tstamp {tstamp}")
                    continue

            for region in regions:
                # Note: yield does not duplicate 'frame', copies pointers
                yield frame, tstamp, region

    @abstractmethod
    def process_images(self, image_batch, tstamp_batch, prev_region_batch=None):
        """Abstract method to be implemented by child module"""
        pass

    def _load_media(self):
        self.media = MediaRetriever(self.request.url)
