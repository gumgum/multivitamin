import os
import json

import glog as log
import urllib
import boto3

DEFAULT_SAMPLE_RATE = 1.0


class Request:
    def __init__(self, request_dict, request_id=None):
        """Data object to encapsulate and cleanse request

        Args:
            request_dict (dict): input request json 
            request_id (str): ID tied to request (esp from AWS SQS)
        """
        if not isinstance(request_dict, dict):
            raise ValueError(f"request_dict is type: {type(request_dict)}, should be of type dict")

        self.request = request_dict
        self.request_id = request_id

    @property
    def url(self):
        return _standardize_url(self.request.get("url"))

    @property
    def sample_rate(self):
        return self.request.get("sample_rate", DEFAULT_SAMPLE_RATE)

    @property
    def bin_encoding(self):
        be = self.request.get("bin_encoding", True)
        if isinstance(be, str):
            be = be.lower() == "true"
        return be

    @property
    def bin_decoding(self):
        de = self.request.get("bin_decoding", True)
        if isinstance(de, str):
            de = de.lower() == "true"
        return de

    @property
    def base64_encoding(self):
        be = self.request.get("base64_encoding", True)
        if isinstance(be, str):
            be = be.lower() == "true"
        return be

    @property
    def prev_response(self):
        return self.request.get("prev_response")

    @property
    def prev_response_url(self):
        return self.request.get("prev_response_url")

    @property
    def dst_url(self):
        return self.request.get("dst_url")

    @property
    def flags(self):
        return self.request.get("flags")

    def __repr__(self):
        return f"request: {self.request}; request_id: {self.request_id}"


def _standardize_url(url):
    log.info("Formatting urls in request")
    if not url:
        raise ValueError("url is None")
    # url=url.replace("&amp;", "&")
    # url=url.replace(" ", "\\ ")
    # url=url.replace("https://", "http://")
    # url=url.replace("s://", "http://")
    # url=url.replace("s:", "http://")
    # url=url.replace("https://", "")
    # url=url.replace("http://s.yimg.com", "https://s.yimg.com")
    # url=url.replace(" ", "%20")
    return url
