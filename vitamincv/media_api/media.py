import os
import sys
import numpy as np
import cv2
import requests
import magic
from io import BytesIO
from PIL import Image
import glog as log
import urllib.parse
from enum import Enum
import math

import importlib.util
if importlib.util.find_spec("pims") and importlib.util.find_spec("av"):
    import pims
else:
    log.warning("PIMS not found")

FRAME_EPS = 0.001
DECIMAL_SIGFIG = 3

class Limitation(Enum):
    MEMORY="memory"
    CPU="cpu"

class FileRetriever():
    '''A generic object for retrieving files
    '''
    def __init__(self, url=None):
        self._url = None
        self._is_local = None
        self._content_type = None
        if url is not None:
            self.url = url

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._content_type = None
        url_scheme = urllib.parse.urlparse(value).scheme

        self._url = value
        if url_scheme == ["", "file"]:
            self._is_local = True
        else:
            self._is_local = False

        if not self.exists:
            self._is_local = None
            self._url = None
            raise FileNotFoundError(value)

    @property
    def exists(self):
        if self.is_local:
            return self._does_local_file_exist()
        else:
            return self._does_remote_file_exist()

    def _does_local_file_exist(self):
        if os.path.isfile(self.filepath):
            return False
        return True

    def _does_remote_file_exist(self):
        try:
            resp = requests.head(self.url)
        except Exception as e:
            return False
        return True

    @property
    def filepath(self):
        return self.url.replace("file://","") if self.is_local else None

    @property
    def content_type(self):
        if self._content_type is None and self.is_local and self.exists:
            mime = magic.Magic(mime=True)
            self._content_type = mime.from_file(self.filepath)

        if self._content_type is None and self.is_remote and self.exists:
            resp = requests.head(self.url)
            self._content_type = resp.headers["Content-Type"]

        return self._content_type

    @property
    def is_local(self):
        return self._is_local

    @property
    def is_remote(self):
        return not self.is_local if isinstance(self.is_local, bool) else False

    @property
    def filename(self):
        return os.path.basename(self.url)

    def download(self, filepath=None, return_filelike=False):
        '''Download file to filepath

        Args:
            filepath (str | optional): Filepath to write file to.
                                If directory, it will take it's original filename
                                and save to that directory
            filelike (bool | optional): To return a filelike object or not

        Returns:
            filelike_obj: A BytesIO object containing the file bytes
                            (only if return_filelike is True)
        '''
        if self.is_remote:
            response = requests.get(self.url)
            filelike_obj = BytesIO(response.content)
        else:
            with open(self.filepath, "rb") as f:
                filelike_obj = BytesIO(f.read())

        path = None
        if isinstance(filepath, str):
            if os.path.isdir(filepath):
                path = "{}/{}".format(filepath, self.filename)
            else:
                path = filepath
            with open(path, "wb") as f:
                f.write(filelike_obj.read())

        if return_filelike is True:
            filelike_obj.seek(0)
            return filelike_obj

class MediaRetriever(FileRetriever):
    def __init__(self, url=None, limitation="memory"):
        super(MediaRetriever, self).__init__(url=url)
        self._cap = None
        self._image = None

        self._limitation_default = Limitation.MEMORY
        self._limitation_options = ["memory", "cpu"]

        self.limitation = limitation

    @FileRetriever.url.setter
    def url(self, value):
        FileRetriever.url.fset(self, value)
        self._cap = None
        self._image = None
        if not self.is_image and not self.is_video:
            raise ValueError("Unsupported Content-Type: {}\n Expected one of these options:\n {}, {}".format(self.content_type, "image/*", "video/*"))

    @property
    def is_video(self):
        if self.content_type is None:
            return False
        return "video" in self.content_type

    @property
    def is_image(self):
        if self.content_type is None:
            return False
        return "image" in self.content_type and "gif" not in self.content_type

    @property
    def limitation(self):
        return self._limitation

    @limitation.setter
    def limitation(self, value):
        if isinstance(value, str):
            value = Limitation(value)

        if isinstance(value, Limitation):
            self._limitation = value

    @property
    def video_capture(self):
        if self._cap is None and self.limitation == Limitation.MEMORY and self.is_video:
            self._cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)

        if self._cap is None and self.limitation == Limitation.CPU and self.is_video:
            try:
                self._cap = pims.Video(self.url)
            except NameError:
                log.warning("PIMS not installed... using OpenCV")
                self._cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)

        return self._cap

    @property
    def fps(self):
        if self.is_image:
            log.error("Images have no fps")
            return None
        if not self.video_capture:
            log.error("VideoCapture is None")
            return None

        if isinstance(self.video_capture, cv2.VideoCapture):
            return self.video_capture.get(cv2.CAP_PROP_FPS)
        if isinstance(self.video_capture, pims.Video):
            return self.video_capture.frame_rate

    @property
    def total_frames(self):
        if self.is_image:
            return 1
        if not self.video_capture:
            log.error("VideoCapture is None")
            return None

        if isinstance(self.video_capture, cv2.VideoCapture):
            return self.video_capture.get(cv2.CAP_PROP_FRAME_COUNT)
        if isinstance(self.video_capture, pims.Video):
            return len(self.video_capture)

    @property
    def length(self):
        if self.is_image:
            return 0
        return float(self.total_frames)/self.fps

    @property
    def shape(self):
        '''Gets height by width by channels for frames
        '''
        if self.is_image:
            return self.image.shape

        if isinstance(self.video_capture, cv2.VideoCapture):
            if not self.video_capture.isOpened():
                self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, 0)
            width  = int(self.video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
            channels = 3 # This shouldn't be hard coded
            return (height, width, channels)
        if isinstance(self.video_capture, pims.Video):
            return self.video_capture.frame_shape

    @property
    def image(self):
        if not self.is_image:
            return None

        if self._image is not None:
            return self._image

        filelike_obj = self.download(return_filelike=True)
        image = np.array(Image.open(filelike_obj))
        if image.shape[2] > 3:
            log.warning("Image has >3 channels. Cropping to 3.")
            image = image[:,:,:3]
        self._image = image[:,:,::-1].copy()
        return self._image

    def tstamp_to_frame_index(self, tstamp):
        return round(tstamp*self.fps)

    def get_frame(self, tstamp=0.0):
        """Return frame if image, or get frame with timestamp in seconds (w/ demicals)

        Note: iterating using get_frames_iterator is significantly faster
        """
        if not self.url:
            raise ValueError("URL not set. Please use med_ret.set_url(\"...\")")

        if self.is_image:
            return self.image
        elif self.is_video:
            if isinstance(self.video_capture, cv2.VideoCapture):
                self.video_capture.set(cv2.CAP_PROP_POS_MSEC, tstamp*1000)
                ret, frame = self.video_capture.read()
            if isinstance(self.video_capture, pims.Video):
                frame_idx = self.tstamp_to_frame_index(tstamp)
                ret = True
                try:
                    frame = self.video_capture[frame_idx]
                    frame = np.array(frame)[:, :, ::-1]  # RGB to BGR
                except ValueError:
                    ret = False

            if not ret:
                return ret
            return frame

    def get_frames_iterator(self, sample_rate=100.0, start_tstamp=0.0, end_tstamp=sys.maxsize):
        """If image, returns a list of length 0 with a tuple (image, tstamp),
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
            raise ValueError("URL not set. Please use med_ret.set_url(\"...\")")
        self.period = 1.0/sample_rate
        if self.is_image:
            return [(self.image, 0.00)]
        elif self.is_video:
            return FramesIterator(self.video_capture, sample_rate, start_tstamp, end_tstamp)

    def get_length(self):
        return self.length

    def get_num_frames(self):
        return self.total_frames

    def get_fps(self):
        return self.fps

    def get_w_h(self):
        return self.shape[0:2][::-1]

class FramesIterator():
    """Frames iterator object for videos

    Usage:
    for frame, tstamp in frames_iter:
        do_something(frame)

    Note: Do not use multiple times/attempt to restart from the start of the video. This feature is incomplete.
    TODO: check to see if start_tstamp & end_tstamp checks cause significant slowing
    """
    def __init__(self, video_cap, sample_rate=100.0, start_tstamp=0.0, end_tstamp=sys.maxsize):
        """Frames iterator constructor

        Args:
            video_cap (cv2.VideoCapture): video capture obj
            sample_rate (float): rate to sample video
            start_tstamp (float): start time for iterating
            end_tstamp (float): end time condition for iterating
        """
        self.cap = video_cap

        fps = None
        if isinstance(self.cap, cv2.VideoCapture):
            fps = self.cap.get(cv2.CAP_PROP_FPS)

        if isinstance(self.cap, pims.Video):
            fps = self.cap.frame_rate

        self.period = max(1.0/sample_rate, 1.0/fps)
        log.info("Period: {}".format(self.period))
        self.start_tstamp = start_tstamp
        self.end_tstamp = end_tstamp
        self.cur_tstamp = self.start_tstamp
        self.first_frame = True

        if isinstance(self.cap, cv2.VideoCapture) and start_tstamp != 0.0:
            self.cap.set(cv2.CAP_PROP_POS_MSEC, start_tstamp*1000)

        log.info("Setting start_tstamp of iterator to {}".format(start_tstamp))

    def __iter__(self):
        self.cur_tstamp = self.start_tstamp
        self.first_frame = True
        if isinstance(self.cap, cv2.VideoCapture) and self.start_tstamp != 0.0:
            self.cap.set(cv2.CAP_PROP_POS_MSEC, self.start_tstamp*1000)
        return self

    def _round_tstamp(self, tstamp):
        tstamp = math.floor(tstamp*100000.0)/100000.0
        return round(tstamp, DECIMAL_SIGFIG)

    def __next__(self):
        ret = True
        while(ret and self.cur_tstamp <= self.end_tstamp):
            if isinstance(self.cap, cv2.VideoCapture):
                tstamp = self.cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
                ret = self.cap.grab()
                if (tstamp - self.cur_tstamp + FRAME_EPS) >= (self.period) or self.first_frame:
                    ret, frame = self.cap.retrieve()
                    self.cur_tstamp = tstamp
                    self.first_frame = False
                    return frame, self._round_tstamp(tstamp)
            if isinstance(self.cap, pims.Video):
                try:
                    frame_idx = round(self.cur_tstamp*self.cap.frame_rate)
                    frame = self.cap[frame_idx]
                    frame = np.array(frame)[:, :, ::-1]
                    self.cur_tstamp += self.period
                    tstamp = frame_idx/self.cap.frame_rate
                    return frame, self._round_tstamp(tstamp)
                except:
                    ret = False

        log.info("No more frames to read")
        raise StopIteration()

if __name__ == "__main__":
    from tabulate import tabulate
    from datetime import datetime
    import random
    from tqdm import tqdm
    print("SPEED TEST!!!")

    VIDEO_URL = "https://s3.amazonaws.com/video-ann-testing/NHL_GAME_VIDEO_NJDMTL_M2_NATIONAL_20180401_1520698069177.t.mp4"
    VIDEO_CODEC_PROB_1 = "https://s3.amazonaws.com/video-ann/538_Pelicans+vs+Thunder+11%3A5-fhj713lbrhi.30-31.mp4"
    VIDEO_CODEC_PROB_2 = "https://s3.amazonaws.com/gumgum-sports-analyst-data/media-files/Replay%20Video%20Capture_2018-11-16_11.52.51-2816an1tb0v.mp4"
    VIDEO_CODEC_PROB_3 = "https://s3.amazonaws.com/gumgum-sports-analyst-data/media-files/1%3A3%20Houston%20Rockets%20at%20Golden%20State%20Warriors-6tgm4my1dr6.mp4"

    VIDEO_URLS = [VIDEO_URL, VIDEO_CODEC_PROB_1, VIDEO_CODEC_PROB_2, VIDEO_CODEC_PROB_3]

    def create_media_retrievers(url):
        efficient_mr = MediaRetriever(VIDEO_URL)
        fast_mr = MediaRetriever(VIDEO_URL, limitation="cpu")
        return efficient_mr, fast_mr

    def _benchmark_get_frame(mrs, num_samples=100, num_tests=100):
        averaged_results = []
        random_samples = np.random.rand(num_samples)
        for mr in mrs:
            random_tstamps = random_samples*mr.length
            results = []
            for _ in tqdm(range(num_tests), desc="get frame"):
                start = datetime.now()
                for tstamp in random_samples:
                    mr.get_frame(tstamp)
                end = datetime.now()
                x = end-start
                results.append(x.total_seconds())
            averaged_results.append(np.average(results))
        return averaged_results

    def _benchmark_frames_iterator(mrs, sample_rate, num_samples=100, num_tests=100):
        averaged_results = []
        random_sample = random.random()
        for mr in mrs:
            random_start = random_sample*mr.length
            results = []
            for _ in tqdm(range(num_tests), desc="iterator test"):
                start = datetime.now()
                for idx, _ in enumerate(mr.get_frames_iterator(sample_rate=sample_rate, start_tstamp=random_start)):
                    if idx >= num_samples:
                        break
                end = datetime.now()
                x = end-start
                results.append(x.total_seconds())
            averaged_results.append(np.average(results))
        return averaged_results

    rs = []
    for url in VIDEO_URLS:
        mrs = create_media_retrievers(url)
        headers = ["Test Type", "Efficient", "Fast"]

        test_order = ["get_frame", "iterator (sample_rate=100)", "iterator (sample_rate=2)", "iterator (sample_rate=0.2)"]
        get_frame_results = _benchmark_get_frame(mrs, num_tests=1)
        iterator_results1 = _benchmark_frames_iterator(mrs, 100, num_tests=1)
        iterator_results2 = _benchmark_frames_iterator(mrs, 2, num_tests=1)
        iterator_results3 = _benchmark_frames_iterator(mrs, 0.2, num_tests=1)

        results = list(zip(test_order, list(zip(get_frame_results, iterator_results1, iterator_results2, iterator_results3))))
        rs.append(results)

    print("\n"*4)
    for url, results in zip(VIDEO_URLS, rs):
        print("\nRESULTS ON: "+url)
        print(tabulate(results ,headers=headers))
