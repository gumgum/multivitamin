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
        log.info(f"Creating ImagesModule with batch_size: {batch_size}")

    def process(self, request, prev_media_data=None):
        """Process the message, calls process_images(batch, tstamps, contours=None)

        Returns:
            str code
        """
        log.info("Processing message")
        super().process(request, prev_media_data)
        self._load_media()
        self.num_problematic_frames = 0
        for image_batch, tstamp_batch, det_batch in self.batch_generator(
            self.preprocess_message()
        ):
            if self.num_problematic_frames >= MAX_PROBLEMATIC_FRAMES:
                log.error("Too Many Problematic Iterations")
                log.error("Returning with error code: " + str(self.code))
                return self.code

            try:
                image_batch = self.preprocess_images(image_batch, det_batch)
                prediction_batch_raw = self.process_images(image_batch)
                prediction_batch = self.postprocess_predictions(prediction_batch_raw)
                for predictions, tstamp, prev_det in zip(
                    prediction_batch, tstamp_batch, det_batch
                ):
                    iterable = self.convert_to_detection(
                        predictions=predictions,
                        tstamp=tstamp,
                        previous_detection=prev_det,
                    )
                    if not isinstance(iterable, Iterable) or isinstance(iterable, dict):
                        iterable = [iterable]

                    for new_det in iterable:
                        self.media_data.detections.append(new_det)
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

    def preprocess_message(self):
        """Parses HTTP message for data

        Yields:
            frame: An image a time tstamp of a video or image
            tstamp: The timestamp associated with the frame
            det: The matching detection object
        """
        frames_iterator = []
        try:
            frames_iterator = self.media.get_frames_iterator(self.request.sample_rate)
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

            log.info("tstamp: " + str(tstamp))
            dets = [None]
            if self.prev_media_data:
                log.info("Processing with previous media_data")
                log.debug(
                    f"tstamp_map keys: {self.prev_media_data.det_tstamp_map.keys()}"
                )
                if tstamp in self.prev_media_data.det_tstamp_map:
                    dets = self.prev_media_data.det_tstamp_map[tstamp]
                    log.info(f"Found {len(dets)} dets from previous media_data")
                else:
                    log.info(f"Did not find a detection for tstamp: {tstamp}")

                if len(dets) == 0:
                    log.debug("No detections for tstamp " + str(tstamp))
                    continue

            for det in dets:
                yield frame, tstamp, det

    def convert_to_detection(self, predictions, tstamp=None, previous_detection=None):
        pass

    def preprocess_images(self, images, contours=None):
        return images

    @abstractmethod
    def process_images(self, images, tstamps, detections_of_interest=None):
        """Abstract method to be implemented by child module"""
        pass

    def postprocess_predictions(self, predictions):
        return predictions

    def _load_media(self):
        self.media = MediaRetriever(self.request.url)
        self.media_data.meta["dims"] = self.media.get_w_h()
