import glog as log

from vitamincv.data.response.data import MediaAnn

#converts MediaAnn object to a dictionary (_dictionary)

"""
# contains a MediaAnn object
# contains a _dictionary
# converts MediaAnn to _dictionary
"""


class ModuleResponse():
    """Response object that Modules populate and return

    Contains:
        - vitamincv.data.response.MediaAnn
        - dict
    
    input can be either MediaAnn or dictionary
    if dictionary, assumed to be matching this schema

    Internal methods
        - 
    """
    def __init__(self, input=None, request=None):
        """Wrapper with utilities around a single response document"""
        log.info("Constructing response")
        if input is None:
            self._init_empty_dict()
        elif isinstance(input, dict):
            self._init_from_dict(input)
        elif isinstance(input, MediaAnn):
            self._init_from_media_ann(input)
        else:
            raise TypeError(f"Unsupported input type: {type(input)}")
            
        self.request = request
        self.tstamp_map = None
        self.url = request.url

    @property
    def dict(self):
        return self._dictionary

    @dict.setter
    def dict(self, dictionary):
        if dictionary is None:
            log.info("Response.dictionary is none, creating empty response")
            self._dictionary = ModuleResponseDict().dict
        else:
            log.info("Response.dictionary is NOT none, creating from previous response")
            self._dictionary = dictionary

    @property
    def frame_anns(self):
        return self._dictionary.get("media_annotation").get("frames_annotation")

    def append_region(self, t, region):
        assert(isinstance(region, dict))
        self._dictionary.get("media_annotation").get("frames_annotation")[t].append(region)

    def append_regions(self, t, regions):
        for region in regions:
            assert(isinstance(region, dict))
        self._dictionary.get("media_annotation").get("frames_annotation")[t].extend(regions)

    def has_frame_anns(self):
        return len(self.frame_anns) > 0

    def append_footprint(self, footprint):
        assert(isinstance(footprint, dict))
        self._dictionary["media_annotation"]["codes"].append(footprint)

    def append_video_ann(self, track):
        assert(isinstance(track, dict))
        self._dictionary["media_annotation"]["tracks_summary"].append(track)

    def append_annotation_tasks(self, annotation_tasks):
        self._dictionary["media_annotation"]["annotation_tasks"].extend(annotation_tasks)

    def append_annotation_task(self, annotation_task):
        self._dictionary["media_annotation"]["annotation_tasks"].append(annotation_task)

    def sort_image_anns_by_timestamp(self):
        tmp = self._dictionary["media_annotation"]["frames_annotation"]
        self._dictionary["media_annotation"]["frames_annotation"] = sorted(
            tmp, key=lambda k: k["t"]
        )

    def sort_tracks_summary_by_timestamp(self):
        tmp = self._dictionary["media_annotation"]["tracks_summary"]
        self._dictionary["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k["t1"])

    def compute_video_desc(self):
        raise NotImplementedError()

    @property
    def url(self):
        return self._dictionary["media_annotation"]["url"]

    @url.setter
    def url(self, url):
        assert isinstance(url, str)
        self._dictionary["media_annotation"]["url"] = url

    @property
    def url_original(self):
        return self._dictionary["media_annotation"]["url_original"]

    @url_original.setter
    def url_original(self, url):
        assert isinstance(url, str)
        self._dictionary["media_annotation"]["url_original"] = url

    @property
    def dims(self):
        return (self.width, self.height)

    @property
    def media_summaries(self):
        """Get full media summary

        Returns:
            list of media_summary objects
        """
        return self._dictionary["media_annotation"]["media_summary"]

    @property
    def footprints(self):
        """Get all footprints

        Returns:
            list of footprints
        """
        return self._dictionary["media_annotation"]["codes"]

    @footprints.setter
    def footprints(self, fps):
        assert isinstance(fps, list)
        self._dictionary["media_annotation"]["codes"] = fps

    @property
    def width(self):
        return self._dictionary["media_annotation"]["w"]

    @width.setter
    def width(self, w):
        assert isinstance(w, int)
        self._dictionary["media_annotation"]["w"] = w

    @property
    def height(self):
        return self._dictionary["media_annotation"]["h"]

    @height.setter
    def height(self, h):
        assert isinstance(h, int)
        self._dictionary["media_annotation"]["h"] = h

    @property
    def timestamps(self):
        return sorted(self._dictionary["media_annotation"]["frames_annotation"].keys())

    @property
    def tracks(self):
        return self._dictionary["media_annotation"]["tracks_summary"]
