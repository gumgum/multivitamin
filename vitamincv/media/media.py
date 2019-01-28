import os
import sys
import numpy as np
import cv2
import requests
import magic
import hashlib
import requests
import boto3
import botocore
from io import BytesIO
from PIL import Image
import glog as log

LOCALFILE_PREFIX = "file://" #TODO: use Pathlib Path objects
FRAME_EPS = 0.001
DECIMAL_SIGFIG = 3

class FramesIterator():
    """Frames iterator object for videos

    Usage:
    for frame, tstamp in frames_iter:
        do_something(frame)

    Note: Do not use multiple times/attempt to restart from the start of the video. This feature is incomplete.
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
        self.period = 1.0/sample_rate
        log.info("Period: {}".format(self.period))
        self.start_tstamp = start_tstamp
        self.end_tstamp = end_tstamp
        if start_tstamp != 0.0:
            self.cap.set(cv2.CAP_PROP_POS_MSEC, start_tstamp*1000)
            log.info("Setting start_tstamp of iterator to {}".format(start_tstamp))

    def __iter__(self):
        self.cur_tstamp = self.start_tstamp
        if self.start_tstamp != 0.0:
            self.cap.set(cv2.CAP_PROP_POS_MSEC, self.start_tstamp*1000)
        self.first_frame = True
        return self

    def __next__(self):
        ret = True
        while(ret and self.cur_tstamp <= self.end_tstamp):
            tstamp = self.cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
            ret = self.cap.grab()
            if (tstamp - self.cur_tstamp + FRAME_EPS) >= (self.period) or self.first_frame:
                ret, frame = self.cap.retrieve()
                self.cur_tstamp = tstamp
                self.first_frame = False
                return (frame, round(tstamp, DECIMAL_SIGFIG))

        log.info("No more frames to read")
        raise StopIteration()
    
    def reset(self):
        pass

class MediaRetriever():
    def __init__(self, url=None):
        self.url = None
        if url:
            self.set_url(url)
        self.period = 1.0

    def set_url(self, url):
        self.is_image = False
        self.is_local = False
        self.is_video = False
        self.url = url
        log.info("Loading media from url: {}...".format(url))
        self._compute_content_info()
        self._download_media()
        log.info("Loading media from url: {}... COMPLETE".format(url))

    def reset(self):
        log.info("Resetting media retriever")
        self.set_url(self.url)

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
            return FramesIterator(self.cap, sample_rate, start_tstamp, end_tstamp)

    def get_frame(self, tstamp=0.0):
        """Return frame if image, or get frame with timestamp in seconds (w/ demicals)

        Note: iterating using get_frames_iterator is significantly faster
        """
        if not self.url:
            raise ValueError("URL not set. Please use med_ret.set_url(\"...\")")

        if self.is_image:
            return self.image
        elif self.is_video:
            self.cap.set(cv2.CAP_PROP_POS_MSEC, tstamp*1000)
            ret, frame = self.cap.read()
            if not ret:
                return ret
            return frame

    def get_num_frames(self):
        if not self.is_video:
            return 1
        if not self.cap:
            log.error("VideoCapture is None")
            return None
        return self.cap.get(cv2.CAP_PROP_FRAME_COUNT)

    def get_fps(self):
        if not self.is_video:
            log.error("Cannot get_fps(), not a video")
            return None
        if not self.cap:
            log.error("VideoCapture is None")
            return None
        return self.cap.get(cv2.CAP_PROP_FPS)

    def get_w_h(self):
        if self.is_image:
            return (self.image.shape[1], self.image.shape[0])
        elif self.is_video:
            if not self.cap.isOpened():
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            width  = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            return width, height

    def _download_media(self):
        """Downloads media
          if image, stores in numpy array (BGR)
          if video, stores in cv2.VideoCapture object
          if local, assumes url prefixed with LOCALFILE_PREFIX
        """
        if self.is_image:
            if self.is_local:
                tmp = cv2.imread(self.url)
                if tmp is None:
                    log.warning("Unable to read: {}".format(self.url))
                    self.image = None
                else:
                    self.image = tmp
            else:
                response = requests.get(self.url)
                if response:
                    _tmp = np.array(Image.open(BytesIO(response.content)))
                    if _tmp.shape[2] > 3:
                      log.warning("Image has >3 channels. Cropping to 3.")
                      _tmp = _tmp[:,:,:3]
                    self.image = _tmp[:,:,::-1].copy() #rgb->bgr
                else:
                    log.warning("Response: {}, unable to download: {}".format(response, self.url))
                    self.image = None
        elif self.is_video:
            self.cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)

    def _set_content_type(self, content_type):
        """Parses and sets content type variables
           Currently excludes gifs from images

           Args:
               content_type: string of content-type in url header
        """
        if "image" in content_type and "gif" not in content_type:
            self.is_image = True
        elif "video" in content_type:
            self.is_video = True
#        elif "binary" in content_type:
#            self.is_image = True
        else:
            raise ValueError("Unsupported Content-Type: {}\n \
                             Expected one of these options:\n \
                             {}, {}".format(content_type,
                                                "image/*", "video/*"))

    def _is_local_file(self):
        if LOCALFILE_PREFIX in self.url:
            self.url = self.url.split(LOCALFILE_PREFIX)[1]
            if not os.path.exists(self.url):
                raise FileNotFoundError(self.url)
            return True
        return False

    def _compute_content_info(self):
        log.info("Retrieving content info for " + self.url)
        content_type=''
        if self._is_local_file():
            mime = magic.Magic(mime=True)
            content_type=mime.from_file(self.url)
            self.is_local = True
        else:
            resp = requests.head(self.url)
            content_type=resp.headers["Content-Type"]
        log.info("Content-Type: {}".format(content_type))
        self._set_content_type(content_type)
        
class FileUploader():
    def __init__(self, headers={}, params={}):
        self.mime_magic = magic.Magic(mime=True)
        self.s3_client = boto3.client('s3',region_name='us-east-1')
        self.s3_resource = boto3.resource('s3')

        self.headers = headers
        self.params = params

    def load_local(self, path):
        file_bytes = open(path, 'rb').read()
        if file_bytes:
            return file_bytes
        else:
            raise Exception('Local Path Not Found: {}'.format(path))

    def load_remote(self, url):
        resp = requests.get(url, headers=self.headers, params=self.params)
        resp.raise_for_status()
        return resp.content

    def get_file_bytes(self, url):
        if 'file://' in url:
            path = url.replace('file://', '')
            file_bytes = self.load_local(path)
        else:
            file_bytes = self.load_remote(url)
        mime_type = self.mime_magic.from_buffer(file_bytes)
        return file_bytes, mime_type

    def upload_to_s3(self, file_bytes, s3_bucket_name, filename):
        try:
            self.s3_resource.Object(s3_bucket_name, filename).load()
        except botocore.exceptions.ClientError as e:
            self.s3_client.upload_fileobj(BytesIO(file_bytes), s3_bucket_name, filename)
        return 'https://s3.amazonaws.com/{}/{}'.format(s3_bucket_name, filename)

    def push_to_s3(self, url, s3_bucket_name, filename):
        """Push media file to S3

        Args:
            url (str): Local or remote url to visual media to be uploaded to s3
            s3_bucket_name (str): Name of s3 bucket to upload to
            filename (str): Desired filename in s3

        Returns:
            s3 url of the uploaded media
            
        """
        file_bytes, mime_type = self.get_file_bytes(url)
        s3_url = self.upload_to_s3(file_bytes, s3_bucket_name, filename)
        return s3_url

class MediaUploader(FileUploader):
    def __init__(self, headers={}, params={}):
        super().__init__(headers=headers, params=params)

    def create_unique_filename(self, media_bytes, mime_type):
        extension = mime_type.split('/')[-1]
        filename = "{}.{}".format(hashlib.sha1(media_bytes).hexdigest(), extension)
        return filename

    def push_to_s3(self, url, s3_bucket_name):
        """Push media file to S3

        Args:
            url (str): Local or remote url to visual media to be uploaded to s3
            s3_bucket_name (str): name of s3 bucket to upload to

        Returns:
            s3 url of the uploaded media

        """
        media_bytes, mime_type = self.get_file_bytes(url)
        media_filename = self.create_unique_filename(media_bytes, mime_type)
        s3_url = self.upload_to_s3(media_bytes, s3_bucket_name, media_filename)
        return s3_url
