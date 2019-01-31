from vitamincv.media_api.media import MediaRetriever

video_url = "https://s3.amazonaws.com/video-ann-testing/Tampa-Bay-Lightning-vs-Montreal-Canadians_03102018_112_108-5min-clip.mp4"
image_url = "https://i.imgur.com/sFTMl2T.jpg"
local_video = ""  # "file://" prefix is required
local_image = ""
video_url2 = "https://s3.amazonaws.com/video-ann-testing/Tampa-Bay-Lightning-vs-Montreal-Canadians_03102018_112_108-5min-clip.mp4"

SAMPLE_RATE = 2.0  # frames per second


def do_something(frame):
    pass


def read_media():
    # image url
    img_ret = MediaRetriever(image_url)
    for frame, tstamp in img_ret.get_frames_iterator():
        do_something(frame)

    # if we know its an image (default arg is tstamp=0.0)
    frame = img_ret.get_frame()

    # video url
    vid_ret = MediaRetriever(video_url)
    for frame, tstamp in vid_ret.get_frames_iterator():
        do_something(frame)

    # re-use the object if desired
    vid_ret.set_url(video_url2)

    # set the sample rate for video reading (defaults to 1.0)
    for frame, tstamp in vid_ret.get_frames_iterator(sample_rate=SAMPLE_RATE):
        do_something(frame)

    # set the start and end point for video reading and sample_rate
    for frame, tstamp in vid_ret.get_frames_iterator(
        sample_rate=SAMPLE_RATE, start_tstamp=4.15, end_tstamp=10.33
    ):
        do_something(frame)

    # jump to a particular timestamp
    frame = vid_ret.get_frame(15.443)


if __name__ == "__main__":
    read_media()
