import os
import sys
import glog as log
import json
from pathlib import Path
from queue import Queue

from multivitamin.data import Request, Response
from multivitamin.apis.comm_api import CommAPI
from multivitamin.data.response.utils import get_current_date

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
                return [Request({"kill_flag": "true"})]
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

        for response in responses:
            assert isinstance(response, Response)

        log.info(f"Pushing {len(responses)} items to folder: {self.pushing_folder}")
        outfns = []
        for res in responses:
            fn = self.get_fn(res)
            if not os.path.exists(os.path.dirname(fn)):
                os.makedirs(os.path.dirname(fn))

            log.info(f"Writing {fn}")
            if res.request.bin_encoding is True:
                with open(fn, "wb") as wf:
                    wf.write(res.to_bytes())
            else:
                with open(fn, "w") as wf:
                    wf.write(json.dumps(res.to_dict(), indent=INDENTATION))
            outfns.append(fn)
        return outfns

    def get_fn(self, response):
        """Create a fn from url string 

        Args:
            response (Response): response
        
        Returns:
            str: unique identifier
        """
        if self.default_file:
            return os.path.join(self.pushing_folder, DEFAULT_FILE)

        extension = "json"
        if response.request.bin_encoding is True:
            extension = "avro"
        media_url = response.url
        media_name = os.path.basename(media_url)
        return os.path.join(
            self.pushing_folder, f"{get_current_date()}", f"{media_name}.{extension}"
        )
