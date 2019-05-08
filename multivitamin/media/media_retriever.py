import sys
import numpy as np
from PIL import Image
import glog as log
import math
from abc import ABC, abstractmethod
from .file_retriever import FileRetriever

FRAME_EPS = 0.001
DECIMAL_SIGFIG = 3


class AbstractMediaRetriever(FileRetriever, ABC):
    """A fileretriever for visual media."""

    def __init__(self, url=None):
        """Init MediaRetriever.

        Args:
            url (str | optional): The local or remote url to a file.

        """
        self._cap = None
        self._image = None
        self._shape = None

        super(AbstractMediaRetriever, self).__init__(url=url)

    @FileRetriever.url.setter
    def url(self, value):
        """Set the image/video url."""
        FileRetriever.url.fset(self, value)
        self._cap = None
        self._image = None
        self._shape = None
        if not self.is_image and not self.is_video:
            raise ValueError(
                "Unsupported Content-Type: {}\n Expected one of these options:\n {}, {}".format(
                    self.content_type, "image/*", "video/*"
                )
            )

        w, h = self.get_w_h()
        if w in [0, None] or h in [0, None]:
            raise ValueError(
                "Unable to load visial media properly: {}".format(self.url)
            )

    @property
    @abstractmethod
    def is_video(self):
        """Check if url is a video."""
        pass

    @property
    @abstractmethod
    def is_image(self):
        """Check if url is an image."""
        pass

    @property
    def video_capture(self):
        """Get object that has all of visual information."""
        if self._cap is None:
            self._create_video_capture()

        return self._cap

    @abstractmethod
    def _create_video_capture(self):
        pass

    @property
    def fps(self):
        """Get the fps of the image/video."""
        if self.is_image:
            log.error("Images have no fps")
            return None

        if not self.video_capture:
            log.error("VideoCapture is None")
            return None

        return self._get_fps_from_video_capture()

    @abstractmethod
    def _get_fps_from_video_capture(self):
        pass

    @property
    def total_frames(self):
        """Get the total number of frames."""
        if self.is_image:
            return 1

        if not self.video_capture:
            log.error("VideoCapture is None")
            return None

        return self._get_num_frames()

    @abstractmethod
    def _get_num_frames(self):
        pass

    @property
    def length(self):
        """Get the temporal length of the image/video."""
        if self.is_image:
            return 0

        if self.fps == 0:
            return 0

        return float(self.total_frames) / self.fps

    @property
    def shape(self):
        """Get height by width by channels for frames."""
        if self._shape is None:
            if self.is_image:
                self._shape = self.image.shape

            if self.is_video:
                self._shape = self._get_video_frame_shape()

        if self._shape is None:
            return (None, None, None)

        return self._shape

    @abstractmethod
    def _get_video_frame_shape(self):
        pass

    @property
    def image(self):
        """Get the image.

        Return `None` if it's a video.

        """
        if not self.is_image:
            return None

        if self._image is not None:
            return self._image

        filelike_obj = self.download(return_filelike=True)
        image = np.array(Image.open(filelike_obj).convert('RGB'))
        if image.shape[2] > 3:
            log.warning("Image has >3 channels. Cropping to 3.")
            image = image[:, :, :3]
        self._image = image[:, :, ::-1].copy()
        return self._image

    def tstamp_to_frame_index(self, tstamp):
        """Convert a timestamp to a frame index.

        Args:
            tstamp (float): A float in seconds.

        Returns:
            int: The nearest frame to that tstamp.

        """
        return round(tstamp * self.fps)

    def get_frame(self, tstamp=0.0):
        """Return frame if image, or get frame with timestamp in seconds (w/ demicals).

        Note: iterating using get_frames_iterator is significantly faster

        """
        if not self.url:
            raise ValueError('URL not set. Please use med_ret.set_url("...")')

        if self.is_image:
            return self.image

        elif self.is_video:
            ret, frame = self._get_frame_from_video(tstamp=tstamp)
            if not ret:
                return ret
            return frame

    @abstractmethod
    def _get_frame_from_video(self, tstamp):
        pass

    @abstractmethod
    def _get_frames_iterator_class(self):
        pass

    def get_frames_iterator(
        self, sample_rate=100.0, start_tstamp=0.0, end_tstamp=sys.maxsize
    ):
        """Get a frames iterator.

        If image, returns a list of length 0 with a tuple (image, tstamp),
        If video, returns a FramesIterator

        Usage in either image or video case:
        for frame, tstamp in med_ret.get_frames_iterator():
            do_something(frame, tstamp)

        Args:
            sample_rate (float): sample rate for extracting frames from video
            start_tstamp (float): starting timestamp for iteration
            end_tstamp (float): ending timestamp for iteration

        Returns:
            iterator

        Raises:
            ValueError: If URL is not set

        """
        if not self.url:
            raise ValueError('URL not set. Please use med_ret.set_url("...")')

        if self.is_image:
            return [(self.image, 0.00)]

        elif self.is_video:
            fi = self._get_frames_iterator_class()
            return fi(self.video_capture,
                      self.fps,
                      sample_rate,
                      start_tstamp,
                      end_tstamp)

    def get_length(self):
        """Get the temporal length of the image/video."""
        return self.length

    def get_num_frames(self):
        """Get the number of frame in image/video."""
        return self.total_frames

    def get_fps(self):
        """Get the fps of the image/video."""
        return self.fps

    def get_w_h(self):
        """Get the width and height the visual media."""
        return self.shape[0:2][::-1]


class AbstractFramesIterator(ABC):
    """Frames iterator object for videos.

    Usage:
    for frame, tstamp in frames_iter:
        do_something(frame)

    Note: Do not use multiple times/attempt to restart from the start of the video. This feature is incomplete.
    TODO: check to see if start_tstamp & end_tstamp checks cause significant slowing

    """

    def __init__(self,
                 video_cap,
                 video_fps,
                 sample_rate=100.0,
                 start_tstamp=0.0,
                 end_tstamp=sys.maxsize):
        """Frames iterator constructor.

        Args:
            video_cap (cv2.VideoCapture): video capture obj
            fps (float): The video fps.
            sample_rate (float): rate to sample video
            start_tstamp (float): start time for iterating
            end_tstamp (float): end time condition for iterating

        """
        self.cap = video_cap
        self.period = max(1.0 / sample_rate, 1.0 / video_fps)
        log.debug("Period: {}".format(self.period))
        self.start_tstamp = start_tstamp
        self.end_tstamp = end_tstamp
        self.cur_tstamp = self.start_tstamp
        self.first_frame = True

        self._move_cursor_to_tstamp(self.start_tstamp)
        log.debug("Setting start_tstamp of iterator to {}".format(start_tstamp))

    @abstractmethod
    def _move_cursor_to_tstamp(self, tstamp):
        pass

    def __iter__(self):
        """Iterate over frames."""
        self.cur_tstamp = self.start_tstamp
        self.first_frame = True
        self._move_cursor_to_tstamp(self.start_tstamp)
        return self

    def _round_tstamp(self, tstamp):
        tstamp = math.floor(tstamp * 100000.0) / 100000.0
        return round(tstamp, DECIMAL_SIGFIG)

    @abstractmethod
    def _get_next_frame(self):
        pass

    def __next__(self):
        """Get next frame."""
        ret = True
        frame = None
        while ret and self.cur_tstamp <= self.end_tstamp:
            ret, frame, tstamp = self._get_next_frame()
            if ret:
                return frame, tstamp

        log.info("No more frames to read")
        raise StopIteration()
