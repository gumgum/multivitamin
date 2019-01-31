from abc import ABC, abstractmethod
import time
import glog as log
import requests as sender


class CommAPI(ABC):
    def __init__(self):
        log.info("Construcing communication api.")
        # log.setLevel("DEBUG")

    @abstractmethod
    def pull(self, n=1):
        log.debug("Pulling " + str(n) + " items")

    @abstractmethod
    def push(self, request_apis):
        n = 1
        if type(request_apis) == type([]):
            n = len(request_apis)
        log.debug("Pushing " + str(n) + " items")
