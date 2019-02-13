from abc import abstractmethod
import traceback
from collections.abc import Iterable

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
            frames_iterator = self.media.get_frames_iterator(self.request.sample_rate)
        except Exception as e:
            self.code = Codes.ERROR_LOADING_MEDIA
            # TODO update response with error code
            return self.response

        self.num_problematic_frames=0
        # self.findrois()#This will update self.detections_of_interest
        batch_images = []
        batch_tstamps = []
        batch_regions = []
        log.info("Processing frames")

        #we go thru the frames
        for i, (frame, tstamp) in enumerate(frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue
            if tstamp is None:
                log.warning("Invalid tstamp")
                continue

            log.info('tstamp: ' + str(tstamp))
            if self.prev_pois is not None:#We are expected1 to focus on previous detections
                # dets=[]
                # if tstamp in self.detections_t_map:
                #     dets=self.detections_t_map[tstamp]
                # if len(dets)==0:
                #     #log.debug("No detections for tstamp " + str(tstamp))
                #     continue
                # batch_detections_of_interest.extend(dets)
                # #we append the frame and the tstamps as many times as necessary to fit the length of dets
                # log.debug('len(dets): ' + str(len(dets)))
                # batch_images.extend([frame] * len(dets))
                # batch_tstamps.extend([tstamp] * len(dets))
                # log.debug('len(batch_detections_of_interest): ' + str(len(batch_detections_of_interest)))
                # log.debug('len(batch_images): ' + str(len(batch_images)))
                # log.debug('len(batch_tstamps): ' + str(len(batch_tstamps)))
            else:#if there is no previous detections of interest we simply append frame and timestamp to the batches.
                batch_images.append(frame)
                batch_tstamps.append(tstamp)            
            while len(batch_images)>=self.batch_size:
                log.debug("batch ready to be processed.")
                try:
                    log.debug("Processing batch")
                    self.process_images(
                        batch_images[:self.batch_size],
                        batch_tstamps[:self.batch_size],
                        batch_regions[:self.batch_size]
                        )
                except ValueError as e:
                    self.num_problematic_frames+=1
                    log.warning('Problem processing frames')
                    if self.num_problematic_frames>=MAX_PROBLEMATIC_FRAMES:
                        log.error(e)
                        self.code = Codes.ERROR_PROCESSING
                        return self.response
                        
                log.debug("Updating batch")
                batch_images = batch_images[self.batch_size:]
                batch_tstamps = batch_tstamps[self.batch_size:]
                batch_detections_of_interest = batch_detections_of_interest[self.batch_size:]
                log.debug('***************')
                log.debug('len(batch_detections_of_interest): ' + str(len(batch_detections_of_interest)))
                log.debug('len(batch_images): ' + str(len(batch_images)))
                log.debug('len(batch_tstamps): ' + str(len(batch_tstamps)))
        log.debug("We are done frame iterating.")
        if len(batch_images)>0:
            log.debug('len(batch_detections_of_interest): ' + str(len(batch_detections_of_interest)))
            log.debug('len(batch_images): ' + str(len(batch_images)))
            log.debug('len(batch_tstamps): ' + str(len(batch_tstamps)))
            log.debug('len(batch_images): ' + str(len(batch_images)))
            self.process_images(
                batch_images, 
                batch_tstamps,
                batch_regions)
        log.debug("Finished processing.")
        return self.response


    def process(self, request, response):
        """Process the message, calls process_images(batch, tstamps, contours=None)

        Returns:
            str code
        """
        log.info("Processing message")
        super().process(request, response)

        try:
            self._load_media()
        except Exception as e:
            self.code = Codes.ERROR_LOADING_MEDIA
            # TODO update response with error code
            return self.response

        self.num_problematic_frames = 0
        for image_batch, tstamp_batch, prev_region_batch in self.batch_generator(self.preprocess_request()):
            if self.num_problematic_frames >= MAX_PROBLEMATIC_FRAMES:
                log.error("Too Many Problematic Iterations")
                log.error("Returning with error code: " + str(self.code))
                self.code = Codes.ERROR_PROCESSING

                # TODO update response with error code
                return self.response

            try:
                self.process_images(image_batch, tstamp_batch, prev_region_batch)
            except Exception as e:
                log.error(e)
                log.error(traceback.print_exc())
                self.num_problematic_frames += 1
                continue

        log.info("Finished processing.")
        return self.response

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
            self.code = Codes.ERROR_LOADING_MEDIA
            return self.response

        for i, (frame, tstamp) in enumerate(frames_iterator):
            if frame is None:
                log.warning("Invalid frame")
                continue

            if tstamp is None:
                log.warning("Invalid tstamp")
                continue

            # if i % 100 == 0:
            log.info(f"tstamp: {tstamp}")

            regions = [None]
            # if self.response.has_frame_anns():
            #     log.info("Processing with previous response with frame_anns")
            #     log.info(self.response.dictionary)
            #     log.info(self.response.tstamp_map)
            #     regions = self.response.tstamp_map.get(tstamp, [None])
            #     log.info(f"Found {len(regions)} regions from previous response")

            #     if len(regions) == 0:
            #         log.debug(f"No detections for tstamp {tstamp}")
            #         continue

            for region in regions:
                yield frame, tstamp, region

    @abstractmethod
    def process_images(self, image_batch, tstamp_batch, prev_region_batch=None):
        """Abstract method to be implemented by child module"""
        pass


