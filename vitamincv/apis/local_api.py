import os
import sys
import glog as log
import json
from pathlib import Path
from queue import Queue

from vitamincv.data.request import Request
from vitamincv.apis.comm_api import CommAPI
from vitamincv.data.response.utils import get_current_date

FILEEXT = ".json"
DEFAULT_FILE = "response" + FILEEXT
INDENTATION = 2


class LocalAPI(CommAPI):
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
                raise ValueError(f"{pulling_folder} does not exist.")
            paths = list(p.glob("**/*{}".format(FILEEXT)))
            for path in paths:
                with path.open() as rf:
                    for row in rf:
                        self.json_queue.put(json.loads(row))

        if not os.path.exists(pushing_folder):
            os.makedirs(pushing_folder)

    def pull(self, n=1):
        """Pull a batch of messages to be processed

        Args:
            n (int): batch size
        
        Returns:
            list (Request): list of Request objects to process
        """
        log.info("Pulling local query jsons.")
        requests = []
        for _ in range(n):
            if self.json_queue.empty():
                log.info("Queue of jsons is empty. Exiting.")
                sys.exit("No more jsons to process. Exiting.")
            m = self.json_queue.get()
            log.info(f"Appending request: {m}")
            requests.append(Request(m))
        return requests

    def push(self, responses):
        """Push a list of Response objects to be written to pushing_folder

        Args:
            request_apis (list[Response]): list of responses to be pushed/written to disk
        
        Returns:
            list[str]: list of response filenames written
        """
        if not isinstance(responses, list):
            responses = [responses]

        log.debug(f"Pushing {len(responses)} items to folder: {self.pushing_folder}")
        outfns = []
        for res in responses:
            json_fn = self.get_json_fn(res)

            if not os.path.exists(os.path.dirname(json_fn)):
                os.makedirs(os.path.dirname(json_fn))
            log.info(f"Writing {json_fn}")
            with open(json_fn, "w") as wf:
                wf.write(json.dumps(res.to_dict(), indent=INDENTATION))
            outfns.append(json_fn)
        return outfns

    def get_json_fn(self, response):
        """Create a fn from url string from avro_api

        Args:
            aapi (avro_api): avro_api of response
        
        Returns:
            str: unique identifier
        """
        if self.default_file:
            return os.path.join(self.pushing_folder, DEFAULT_FILE)

        media_url = response.url
        media_name = os.path.basename(media_url)
        return os.path.join(self.pushing_folder, f"{get_current_date}", f"{media_name}.json")
