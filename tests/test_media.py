import numpy as np
import requests.exceptions
import pytest
import random
import glog as log

from vitamincv.media_api import media

VIDEO_URL = "https://s3.amazonaws.com/video-ann-testing/NHL_GAME_VIDEO_NJDMTL_M2_NATIONAL_20180401_1520698069177.t.mp4"
IMAGE_URL = "https://upload.wikimedia.org/wikipedia/commons/0/0b/Cat_poster_1.jpg"

def test_bad_url():
    log.info("Testing erronous URL")
    with pytest.raises(FileNotFoundError):
        med_ret = media.MediaRetriever("blah")

def test_not_a_media_url():
    log.info("Testing URL with no media")
    with pytest.raises(ValueError):
        med_ret = media.MediaRetriever("http://www.google.com")

def test_not_a_valid_filepath():
    log.info("Testing localfile of invalid filepath")
    with pytest.raises(FileNotFoundError):
        med_ret = media.MediaRetriever("file://fhekslf")

def test_attributes():
    global efficient_mr, fast_mr
    efficient_mr = media.MediaRetriever(VIDEO_URL)
    fast_mr = media.MediaRetriever(VIDEO_URL, limitation="cpu")

    assert(efficient_mr.get_fps() == fast_mr.get_fps())
    assert(efficient_mr.get_num_frames() == fast_mr.get_num_frames())
    assert(efficient_mr.get_length() == fast_mr.get_length())
    assert(efficient_mr.get_w_h() == fast_mr.get_w_h())

def test_image():
    mr = media.MediaRetriever(IMAGE_URL)
<<<<<<< HEAD
    im = mr.image
=======
    im = mr.get_frame()
>>>>>>> 602c03025115001509214ed50ca7739014cd1d82
    assert(im is not None)

def test_download():
    mr = media.MediaRetriever(IMAGE_URL)
    filelike_obj = mr.download(return_filelike=True)
    assert(filelike_obj)
    assert(len(filelike_obj.read()) > 0)

def test_equivalence_inmem_cv2():
    pass

def test_get_frame():
    assert(efficient_mr.get_length() == fast_mr.get_length())
    length = efficient_mr.get_length()
    random_tstamp = length*random.random()
    im1 = efficient_mr.get_frame(random_tstamp)
    im2 = fast_mr.get_frame(random_tstamp)
    assert(np.array_equal(im1, im2))

def test_frames_iterator():
    assert(efficient_mr.get_length() == fast_mr.get_length())
    length = efficient_mr.get_length()
    random_tstamp1 = length*random.random()
    random_tstamp2 = length*random.random()
    sample_rate1 = 0.1*efficient_mr.get_fps()
    sample_rate2 = 10*efficient_mr.get_fps()
    _run_frames_iterator(sample_rate1, min(random_tstamp1, random_tstamp2), max(random_tstamp1, random_tstamp2))
    _run_frames_iterator(sample_rate2, min(random_tstamp1, random_tstamp2), max(random_tstamp1, random_tstamp2))

def _run_frames_iterator(sample_rate, start, stop):
    efficient_iterator = efficient_mr.get_frames_iterator(sample_rate=sample_rate,
                                                start_tstamp=start,
                                                end_tstamp=stop)
    fast_iterator = fast_mr.get_frames_iterator(sample_rate=sample_rate,
                                                start_tstamp=start,
                                                end_tstamp=stop)
    stopped1 = False
    stopped2 = False
    for idx in range(100):
        log.info(idx)
        try:
            im1, t1 = next(efficient_iterator)
        except StopIteration:
            log.info("Efficient Video Ended Early")
            stopped2 = True

        try:
            im2, t2 = next(fast_iterator)
        except StopIteration:
            log.info("Fast Video Ended Early")
            stopped2 = True

        assert(stopped1 == stopped2)
        assert(t1 == t2)
        assert(np.array_equal(im1, im2))

def test_consistency_between_get_frame_and_frames_iterator():
    assert(efficient_mr.get_length() == fast_mr.get_length())
    length = efficient_mr.get_length()
    random_tstamp = length*random.random()
    efficient_iterator = efficient_mr.get_frames_iterator(start_tstamp=random_tstamp)
    fast_iterator = fast_mr.get_frames_iterator(start_tstamp=random_tstamp)
    im1_iter, t1_iter = next(efficient_iterator)
    im2_iter, t2_iter = next(fast_iterator)
    im1 = efficient_mr.get_frame(t1_iter)
    im2 = fast_mr.get_frame(t2_iter)
    assert(np.array_equal(im1_iter, im1))
    assert(np.array_equal(im2_iter, im2))
    assert(np.array_equal(im1, im2))
