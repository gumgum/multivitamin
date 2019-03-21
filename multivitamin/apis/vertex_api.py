import traceback
import glog as log
import credstash
import requests as sender

from multivitamin.apis.sqs_api import SQSAPI
from multivitamin.data import Response


class VertexAPI(SQSAPI):
    def __init__(self, queue_name):
        """Vertex API inherits from SQSAPI. Pulls messages from SQS queue, posts messages to dst_url

        Args:
            queue_name (str): AWS SQS queue name
        """
        super().__init__(queue_name)
        auth_header = credstash.getSecret(
            "vertex-api-auth-header", table="VA-CredStash-ImageScience-Vertex"
        )
        auth_header = auth_header.split(":")
        self.auth_header = {auth_header[0]: auth_header[1].strip()}

    def pull(self, n=1):
        return super().pull(n)

    def push(self, responses, dst_url=None, delete_flag=True):
        """Pushes responses to a destination URL via the requests lib and deletes SQS message
           based on request_id
        
        Args:
            responses (List[Response]): list of Response objects
            dst_url (str): url for POST endpoint
            delete_flag (bool): bool to delete request_id from SQS
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
                log.info("No dst_url in request. Not pushing response.")
            if delete_flag:
                log.info(f"Deleting {res.request.request_id} from {self.queue_url}")
                super().delete_message(res.request.request_id)
