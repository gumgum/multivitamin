from abc import ABC, abstractmethod
import time

import glog as log
import credstash
import requests as sender

from vitamincv.apis.sqs_api import SQSAPI


class VertexAPI(SQSAPI):
    def __init__(self, queue_name):
        """Vertex API inherits from SQSAPI

        Args:
            queue_name (str): AWS SQS queue name
        """
        # log.setLevel("DEBUG")
        super().__init__(queue_name)
        auth_header = credstash.getSecret(
            "vertex-api-auth-header", table="VA-CredStash-ImageScience-Vertex"
        )
        auth_header = auth_header.split(":")
        self.auth_header = {auth_header[0]: auth_header[1].strip()}

    def pull(self, n=1):
        return super().pull(n)

    def push(self, responses, dst_url=None, delete_flag=True):
        if not isinstance(responses, list):
            responses = [responses]

        log.debug("Pushing " + str(len(request_apis)) + " items")
        for res in responses:
            if dst_url:
                log.info(f"Pushing to {dst_url}")
                ret = sender.post(dst_url, headers=self.auth_header, data=res.to_dict())
                log.info(f"requests.post(...) response: {ret}")
            else:
                log.info("No dst_url in request. Not pushing response.")
            if delete_flag:
                log.info(
                    "Deleting "
                    + r.get_request_id()
                    + " from queue "
                    + str(self.queue_url)
                )
                super().delete_message(r.get_request_id())
