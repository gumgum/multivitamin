### Installation

Requires opencv 3 compiled from sources

Usage:
```
media_retriever = MediaRetriever(url)
#url can be either a url or a localfile prefixed with file://
#currently does not support gifs, and must either have an image or video content-type tag

#usage option 1:
for frame, tstamp in media_retriever.get_frames_iterator():
    do_something(...)
    #frame is a numpy.ndarray

#usage option 2:
frame = get_frame(tstamp=1.55)
```

