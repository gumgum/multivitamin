import glog as log
import boto3
import json

from multivitamin.data import Request
from multivitamin.apis.comm_api import CommAPI
from multivitamin.apis import config

boto3.client("iam")  # required to expose iam roles to boto3?


class SQSAPI(CommAPI):
    def __init__(self, queue_name):
        """Class to pull messages from and push messages to SQS 

        Args:
            queue_name (str): SQS name in AI account
        """
        assert(isinstance(queue_name, str))
        self.sqs = boto3.client("sqs")
        log.info(f"Attempting to retrieve queue: {queue_name}")
        try:
            queue = self.sqs.get_queue_url(QueueName=queue_name)
            log.info(f"Retrieved queue: {queue}")
        except Exception:
            log.info("The queue does not exist, creating it")
            queue = self.sqs.create_queue(
                QueueName=queue_name, Attributes={"DelaySeconds": "120"}
            )
        self.queue_url = queue["QueueUrl"]
        log.info(f"Retrieved queue_url: {self.queue_url}")

    def pull(self, n=1):
        """Pull messages from SQS queue

        Args:
            n (int): batch pulling. NOT IMPLEMENTED
        
        Returns:
            list[Request]: list of requests
        """
        log.info(f"Polling request from queue {self.queue_url}...")
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url, WaitTimeSeconds=config.SQS_WAIT_TIME_SEC
        )
        while "Messages" not in response:
            log.info(f"Polling request from queue {self.queue_url}...")
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url, WaitTimeSeconds=config.SQS_WAIT_TIME_SEC
            )
        log.info(f"sqs.receive_message response: {response}")
        requests = []
        for m in response["Messages"]:
            log.info(str(m))
            requests.append(
                Request(request_dict=json.loads(m["Body"]), request_id=m["ReceiptHandle"])
            )
        return requests

    def push(self, messages):
        """Push messages into SQS queue

        Args:
            messages (list[str]): list of messages in str json format
        """
        for m in messages:
            response = self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=m)
            log.info(f"sqs.send_message response: {response}")

    def delete_message(self, request_id):
        """Delete a message from the SQS queue given a request_id

        Args:
            request_id: request_id from sqs.receive_message
        """
        self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=request_id)
