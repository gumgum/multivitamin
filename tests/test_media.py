import context
import sys
import numpy as np
import cv2
import time
import requests.exceptions
import pytest
import glog as log

from cvapis.media_api import media

def test_bad_url():
    log.info("Testing erronous URL")
    with pytest.raises(requests.exceptions.MissingSchema):
        med_ret = media.MediaRetriever("blah")

def test_not_a_media_url():
    log.info("Testing URL with no media")
    with pytest.raises(ValueError):
        med_ret = media.MediaRetriever("http://www.google.com")

def test_not_a_valid_filepath():
    log.info("Testing localfile of invalid filepath")
    with pytest.raises(FileNotFoundError):
        med_ret = media.MediaRetriever("file://fhekslf")

arbitrary_frame_nr = 2500
video_url = "https://s3.amazonaws.com/video-ann-testing/NHL_GAME_VIDEO_NJDMTL_M2_NATIONAL_20180401_1520698069177.t.mp4"

def test_frame_seek_error(video_url=video_url):
    log.info("Testing frame seek equivalency for frame num: {} for URL: {}".format(arbitrary_frame_nr, video_url))
    start = time.time()
    mr = media.MediaRetriever(video_url)
    src_tstamp = 0
    src_frame = None
    for i, (frame, tstamp) in enumerate(mr.get_frames_iterator(100.0)):
        if i >= arbitrary_frame_nr:
            src_tstamp = tstamp
            src_frame = frame
            log.info("breaking at tstamp: {}".format(src_tstamp))
            break

    mr2 = media.MediaRetriever(video_url)
    log.info("measuring seek vs iteration on tstamp: {}".format(src_tstamp))
    frame = mr2.get_frame(src_tstamp)
    pixel_diff = np.sum(frame-src_frame)
    log.info("pixel difference between seek and iteration: {}".format(pixel_diff))
    log.info("time elapsed: {}".format(time.time()-start))
    assert(pixel_diff == 0)
    log.info("passed")


