from abc import ABC,abstractmethod
import time

import glog as log
import credstash
import requests as sender

from cvapis.comm_apis.sqs_api import SQSAPI

class VertexAPI(SQSAPI):
    def __init__(self, queue_name):
        """Vertex API inherits from SQSAPI

        Args:
            queue_name (str): AWS SQS queue name
        """
        #log.setLevel("DEBUG")
        super().__init__(queue_name)
        auth_header = credstash.getSecret('vertex-api-auth-header', table='VA-CredStash-ImageScience-Vertex')
        auth_header = auth_header.split(":")
        self.auth_header = {auth_header[0]: auth_header[1].strip()}

    def pull(self, n=1):
        return super().pull(n)

    def push(self, request_apis, delete_flag=True):
        if type(request_apis)!=type([]):
            request_apis=[request_apis]
        log.debug("Pushing " + str(len(request_apis)) + " items")
        for r in request_apis:
            response=r.get_response()            
            dst_url=r.get_destination_url()#This will include the request id as a parameter of the url
            if dst_url:
                log.info("Pushing to {}".format(dst_url))
                ret = sender.post(dst_url, headers=self.auth_header, data=response)
                log.info("requests.post(...) response: {}".format(ret))
            else:
                log.info("No dst_url in request. Not pushing response.")
            if delete_flag:
                log.info("Deleting "+r.get_request_id()+ " from queue " + str(self.queue_url))
                super().delete_message(r.get_request_id())


