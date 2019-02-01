import os
import json

import glog as log
import urllib
import boto3

from vitamincv.data.config import DEFAULT_SAMPLE_RATE


class Request:
    def __init__(self, request_dict, request_id=None):
        """Object to encapsulate and cleanse request

        Args:
            request_dict (dict): input request json 
            request_id (str): ID tied to request (esp from AWS SQS)
        """
        if not isinstance(request_dict, dict):
            raise ValueError(f"request_dict is type: {type(request_dict)}, should be of type dict")

        log.debug(f"request_dict: {request_dict}")
        self.request = request_dict
        self.request_id = request_id
        self._load_params(self.request)
        self.url = _standardize_url(self.request.get("url"))

    def _load_params(self, req):
        """Load request parameters. 
        
        Checks for required parameters and sets defaults to optional params

        Args:
            req (dict): request
        
        Returns:
            dict: request
        """
        if self.request.get("url") is None:
            log.info("No URL present in request")
            raise ValueError("No URL present in request")

        self.sample_rate = self.request.get("sample_rate", DEFAULT_SAMPLE_RATE)
        log.info(f"Setting self.sample_rate to {self.sample_rate}")

        self.bin_encoding = self.request.get("bin_encoding", True)
        log.info(f"Setting self.bin_encoding to {self.bin_encoding}")

        self.bin_decoding = self.request.get("bin_decoding", True)
        log.info(f"Setting self.bin_decoding to {self.bin_decoding}")

        self.prev_response = self.request.get("prev_response")
        log.info(f"Setting self.prev_response to {self.prev_response}")

        self.prev_response_url = self.request.get("prev_response_url")
        log.info(f"Setting self.prev_response_url to {self.prev_response_url}")

        self.dst_url = self.request.get("dst_url")
        log.info(f"Setting self.dst_url to {self.dst_url}")

        self.flags = self.request.get("flags")
        log.info(f"Setting self.flags to {self.flags}")


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
