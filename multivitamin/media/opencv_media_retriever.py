import cv2
import sys

from .media_retriever import AbstractMediaRetriever, AbstractFramesIterator
from .media_retriever import FRAME_EPS


class OpenCVMediaRetriever(AbstractMediaRetriever):
    """A fileretriever for visual media."""

    def __init__(self, url=None):
        """Init MediaRetriever.

        Args:
            url (str | optional): The local or remote url to a file

        """
        super(OpenCVMediaRetriever, self).__init__(url=url)

    @property
    def is_video(self):
        """Check if url is a video."""
        if self.content_type is None:
            return False
        return "video" in self.content_type

    @property
    def is_image(self):
        """Check if url is an image."""
        if self.content_type is None:
            return False
        return "image" in self.content_type and "gif" not in self.content_type

    def _create_video_capture(self):
        """Get the used video capture."""
        if self.is_video:
            self._cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)

        return self._cap

    def _get_fps_from_video_capture(self):
        """Get the fps of the image/video."""
        return self.video_capture.get(cv2.CAP_PROP_FPS)

    def _get_num_frames(self):
        """Get the total number of frames."""
        return self.video_capture.get(cv2.CAP_PROP_FRAME_COUNT)

    def _get_video_frame_shape(self):
        """Get height by width by channels for frames."""
        if not self.video_capture.isOpened():
            self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, 0)

        f = self.get_frame()
        return f.shape

    def _get_frame_from_video(self, tstamp):
        """Return frame if image, or get frame with timestamp in seconds (w/ demicals).

        Note: iterating using get_frames_iterator is significantly faster

        """
        self.video_capture.set(cv2.CAP_PROP_POS_MSEC, tstamp * 1000)
        ret, frame = self.video_capture.read()
        return ret, frame

    def _get_frames_iterator_class(self):
        return OpenCVFramesIterator


class OpenCVFramesIterator(AbstractFramesIterator):
    """Frames iterator object for videos.

    Usage:
    for frame, tstamp in frames_iter:
        do_something(frame)

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
            sample_rate (float): rate to sample video
            start_tstamp (float): start time for iterating
            end_tstamp (float): end time condition for iterating

        """
        super(OpenCVFramesIterator, self).__init__(video_cap,
                                                   video_fps=video_fps,
                                                   sample_rate=sample_rate,
                                                   start_tstamp=start_tstamp,
                                                   end_tstamp=end_tstamp)

    def _move_cursor_to_tstamp(self, tstamp):
        """Iterate over frames."""
        self.cap.set(cv2.CAP_PROP_POS_MSEC, tstamp * 1000)

    def _get_next_frame(self):
        ret = True
        while ret and self.cur_tstamp <= self.end_tstamp:
            tstamp = self.cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
            ret = self.cap.grab()
            if (tstamp - self.cur_tstamp + FRAME_EPS) >= (
                self.period
            ) or self.first_frame:
                ret, frame = self.cap.retrieve()
                self.cur_tstamp = tstamp
                self.first_frame = False
                return ret, frame, self._round_tstamp(tstamp)
        return ret, None, None
