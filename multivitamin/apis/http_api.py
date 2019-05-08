import traceback
import glog as log
import credstash
import requests as sender

from multivitamin.apis import CommAPI
from multivitamin.data import Response


class HTTPAPI(CommAPI):
    def pull(self, n=1):
        raise NotImplementedError()

    def push(self, responses, dst_url=None):
        """Pushes responses to a destination URL via the requests lib

        Args:
            responses (List[Response]): list of Response objects
            dst_url (str): url for POST endpoint
        """
        if not isinstance(responses, list):
            responses = [responses]

        log.debug(f"Pushing {len(responses)} items")
        for res in responses:
            assert isinstance(res, Response)
            if dst_url is None:
                dst_url = res.request.dst_url
            if dst_url:
                log.info(f"Pushing to {dst_url}")
                data = res.data
                log.info(f"data is of type {type(data)}")
                try:
                    ret = sender.post(dst_url, headers=self.auth_header, data=data)
                    log.info(f"requests.post(...) response: {ret}")
                except Exception as e:
                    log.error(e)
                    log.error(traceback.print_exc())
            else:
                log.info("No dst_url field in request. Not pushing response.")
