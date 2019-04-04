import sys
import pims
import numpy as np
from .media_retriever import AbstractMediaRetriever, AbstractFramesIterator


class PIMSMediaRetriever(AbstractMediaRetriever):
    """A fileretriever for visual media."""

    def __init__(self, url=None):
        """Init MediaRetriever.

        Args:
            url (str | optional): The local or remote url to a file

        """
        super(AbstractMediaRetriever, self).__init__(url=url)

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
        if self._cap is None:
            self._cap = pims.Video(self.url)

        return self._cap

    @property
    def _get_fps_from_video_capture(self):
        return self.video_capture.frame_rate

    @property
    def _get_num_frames(self):
        """Get the total number of frames."""
        return len(self.video_capture)

    @property
    def _get_video_frame_shape(self):
        """Get height by width by channels for frames."""
        shape = self.video_capture.frame_shape
        return shape

    def _get_frame_from_video(self, tstamp=0.0):
        """Return frame if image, or get frame with timestamp in seconds (w/ demicals).

        Note: iterating using get_frames_iterator is significantly faster

        """
        frame_idx = self.tstamp_to_frame_index(tstamp)
        ret = True
        frame = None
        try:
            frame = self.video_capture[frame_idx]
            frame = np.array(frame)[:, :, ::-1]  # RGB to BGR
        except ValueError:
            ret = False

        return ret, frame

    def _get_frames_iterator_class(self):
        return PIMSFramesIterator


class PIMSFramesIterator(AbstractFramesIterator):
    """Frames iterator object for videos."""

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
        super(PIMSFramesIterator, self).__init__(video_cap,
                                                 video_fps=video_fps,
                                                 sample_rate=sample_rate,
                                                 start_tstamp=start_tstamp,
                                                 end_tstamp=end_tstamp)

    def _move_cursor_to_tstamp(self):
        """Iterate over frames."""
        pass

    def _get_next_frame(self):
        """Get next frame."""
        ret = True
        frame = None
        tstamp = None
        try:
            frame_idx = round(self.cur_tstamp * self.cap.frame_rate)
            frame = self.cap[frame_idx]
            frame = np.array(frame)[:, :, ::-1]
            self.cur_tstamp += self.period
            tstamp = frame_idx / self.cap.frame_rate
            return frame, self._round_tstamp(tstamp)
        except ValueError:
            ret = False

        return ret, frame, tstamp
