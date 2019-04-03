import numpy as np
import pytest
import random
import glog as log

from multivitamin.media import OpenCVMediaRetriever

VIDEO_URL = "https://s3.amazonaws.com/video-ann-testing/NHL_GAME_VIDEO_NJDMTL_M2_NATIONAL_20180401_1520698069177.t.mp4"
IMAGE_URL = "https://upload.wikimedia.org/wikipedia/commons/0/0b/Cat_poster_1.jpg"

VIDEO_CODEC_PROB_1 = "https://s3.amazonaws.com/video-ann/538_Pelicans+vs+Thunder+11%3A5-fhj713lbrhi.30-31.mp4"
VIDEO_CODEC_PROB_2 = "https://s3.amazonaws.com/gumgum-sports-analyst-data/media-files/Replay%20Video%20Capture_2018-11-16_11.52.51-2816an1tb0v.mp4"
VIDEO_CODEC_PROB_3 = "https://s3.amazonaws.com/gumgum-sports-analyst-data/media-files/1%3A3%20Houston%20Rockets%20at%20Golden%20State%20Warriors-6tgm4my1dr6.mp4"

VIDEO_URLS = [VIDEO_URL, VIDEO_CODEC_PROB_1, VIDEO_CODEC_PROB_2, VIDEO_CODEC_PROB_3]


def test_bad_url():
    with pytest.raises(FileNotFoundError):
        med_ret = OpenCVMediaRetriever("blah")


def test_not_a_media_url():
    with pytest.raises(ValueError):
        med_ret = OpenCVMediaRetriever("http://www.google.com")


def test_not_a_valid_filepath():
    with pytest.raises(FileNotFoundError):
        med_ret = OpenCVMediaRetriever("file://fhekslf")


@pytest.mark.parametrize("video_url", VIDEO_URLS)
def test_attributes(video_url):
    mr = OpenCVMediaRetriever(video_url)

    assert(mr.get_fps() > 0)
    assert(mr.get_num_frames() > 0)
    assert(mr.get_length() > 0)
    w, h = mr.get_w_h()
    assert(w > h > 0)  # Assume all videos are wider than tall.


def test_image():
    mr = OpenCVMediaRetriever(IMAGE_URL)
    assert(mr.image is not None)
    assert(mr.is_image)


def test_download():
    mr = OpenCVMediaRetriever(IMAGE_URL)
    filelike_obj = mr.download(return_filelike=True)
    assert filelike_obj
    assert len(filelike_obj.read()) > 0


# @pytest.mark.parametrize("video_url", VIDEO_URLS)
# def test_get_frame(video_url):
#     efficient_mr, fast_mr = create_media_retrievers(video_url)
#     assert efficient_mr.get_length() == fast_mr.get_length()
#     length = efficient_mr.get_length()
#     random_tstamp = length * random.random()
#     im1 = efficient_mr.get_frame(random_tstamp)
#     im2 = fast_mr.get_frame(random_tstamp)
#     assert np.array_equal(im1, im2)
#
#
# @pytest.mark.parametrize("video_url", VIDEO_URLS)
# def test_frames_iterator(video_url):
#     efficient_mr, fast_mr = create_media_retrievers(video_url)
#     assert efficient_mr.get_length() == fast_mr.get_length()
#     length = efficient_mr.get_length()
#     random_tstamp1 = length * random.random()
#     random_tstamp2 = length * random.random()
#     sample_rate1 = 0.1 * efficient_mr.get_fps()
#     sample_rate2 = 10 * efficient_mr.get_fps()
#     _run_frames_iterator(
#         efficient_mr,
#         fast_mr,
#         sample_rate1,
#         min(random_tstamp1, random_tstamp2),
#         max(random_tstamp1, random_tstamp2),
#     )
#     _run_frames_iterator(
#         efficient_mr,
#         fast_mr,
#         sample_rate2,
#         min(random_tstamp1, random_tstamp2),
#         max(random_tstamp1, random_tstamp2),
#     )
#
#
# def _run_frames_iterator(efficient_mr, fast_mr, sample_rate, start, stop):
#     efficient_iterator = efficient_mr.get_frames_iterator(
#         sample_rate=sample_rate, start_tstamp=start, end_tstamp=stop
#     )
#     fast_iterator = fast_mr.get_frames_iterator(
#         sample_rate=sample_rate, start_tstamp=start, end_tstamp=stop
#     )
#     stopped1 = False
#     stopped2 = False
#     for idx in range(100):
#         log.info(idx)
#         try:
#             im1, t1 = next(efficient_iterator)
#         except StopIteration:
#             log.info("Efficient Video Ended Early")
#             stopped1 = True
#
#         try:
#             im2, t2 = next(fast_iterator)
#         except StopIteration:
#             log.info("Fast Video Ended Early")
#             stopped2 = True
#
#         assert stopped1 == stopped2
#         assert np.array_equal(im1, im2)
#         assert t1 == t2
#
#
# @pytest.mark.parametrize("video_url", VIDEO_URLS)
# def test_consistency_between_get_frame_and_frames_iterator(video_url):
#     efficient_mr, fast_mr = create_media_retrievers(video_url)
#     assert efficient_mr.get_length() == fast_mr.get_length()
#     length = efficient_mr.get_length()
#     random_tstamp = length * random.random()
#     efficient_iterator = efficient_mr.get_frames_iterator(start_tstamp=random_tstamp)
#     fast_iterator = fast_mr.get_frames_iterator(start_tstamp=random_tstamp)
#     im1_iter, t1_iter = next(efficient_iterator)
#     im2_iter, t2_iter = next(fast_iterator)
#     im1 = efficient_mr.get_frame(t1_iter)
#     im2 = fast_mr.get_frame(t2_iter)
#     assert np.array_equal(im1_iter, im1)
#     assert np.array_equal(im2_iter, im2)
#     assert np.array_equal(im1, im2)
