import os 
import sys
import glog as log
import json 
from pathlib import Path
from queue import Queue

from vitamincv.comm_apis.request_api import RequestAPI
from vitamincv.comm_apis.comm_api import AsyncAPI
from vitamincv.avro_api.utils import get_current_date


FILEEXT=".json"
DEFAULT_FILE = "response" + FILEEXT
INDENTATION=2

class LocalAPI(AsyncAPI):
    def __init__(self, pulling_folder=None, pushing_folder=None, default_file=False):
        """LocalAPI is a CommAPI object that pulls queries from a local file
        and pushes responses to a file

        Args:
            pulling_folder (str): folder of jsons defining query, assumes *.json suffix
            pushing_folder (str): where to write responses
            default_file (bool): write to response.json and overwrite each time
        """
        self.pushing_folder = pushing_folder
        self.json_queue = Queue()
        self.default_file = default_file

        if pulling_folder:
            p = Path(pulling_folder)
            if not p.exists():
                raise ValueError("{} does not exist.".format(pulling_folder))
            paths = list(p.glob('**/*{}'.format(FILEEXT)))
            for path in paths:
                with path.open() as rf:
                    for row in rf:
                        self.json_queue.put(row)
        
        if not os.path.exists(pushing_folder):
            os.makedirs(pushing_folder)

    def pull(self, n=1):
        """Pull a batch of messages to be processed

        Args:
            n (int): batch size
        
        Returns:
            list (RequestAPI): list of RequestAPI objects to process
        """
        log.info("Pulling local query jsons.")
        requests = []
        for _ in range(n):
            if self.json_queue.empty():
                log.info("Queue of jsons is empty. Exiting.")
                sys.exit("No more jsons to process. Exiting.")
            m = self.json_queue.get()
            log.info("Appending request: {}".format(m))
            requests.append(RequestAPI(request=m))
        return requests

    def push(self, request_apis):
        """Push a list of RequestAPI objects to be written to pushing_folder

        Args:
            request_apis (list[RequestAPI]): list of requests to be pushed/written to disk
        
        Returns:
            list[str]: list of response filenames written
        """
        if not isinstance(request_apis, list):
            request_apis = [request_apis]

        log.debug("Pushing {} items to folder: {}".format(len(request_apis), self.pushing_folder))
        outfns = []
        for r in request_apis:
            response = r.get_response()
            json_fn = self.get_json_fn(r.avro_api)

            if not os.path.exists(os.path.dirname(json_fn)):
                os.makedirs(os.path.dirname(json_fn))
            log.info("Writing {}".format(json_fn))
            with open(json_fn, 'w') as wf:
                wf.write(r.get_response(indent=INDENTATION))
            outfns.append(json_fn)
        return outfns

    def get_json_fn(self, aapi):
        """Create a fn from url string from avro_api

        Args:
            aapi (avro_api): avro_api of response
        
        Returns:
            str: unique identifier
        """
        if self.default_file:
            return os.path.join(self.pushing_folder, DEFAULT_FILE)

        media_url = aapi.get_url()
        media_name = os.path.splitext(os.path.basename(media_url))[0]
        return os.path.join(self.pushing_folder, "{}".format(get_current_date()), "{}.json".format(media_name))

