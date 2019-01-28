import os
import sys
import json

class Response():
    def __init__(self, bin_encoding=True, bin_decoding=True, prev_response=None, prev_response_url=None):
        log.info("Initializing response object")
        if prev_response and prev_response_url:
            raise ValueError(f"request contains both prev_response and prev_response_url")
        
        log.info("Request contains previous response url")
        