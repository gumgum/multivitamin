import glog as log
import boto3
import json

from vitamincv.data.request import Request
from vitamincv.apis.comm_api import CommAPI
from vitamincv.apis import config

boto3.client('iam') #required to expose iam roles to boto3?

class SQSAPI(CommAPI):
    """Class to pull messages from and push messages to SQS """
    def __init__(self, queue_name):
        """ Construction requires a queue_name
        Args:
            queue_name: SQS name in AI account
           
        """
        super().__init__()
        self.sqs = boto3.client('sqs')
        log.info("Attempting to retrieve queue: {}".format(queue_name))
        try:
            queue = self.sqs.get_queue_url(QueueName=queue_name)
            log.info("Retrieved queue: {}".format(queue))
        except:
            log.info('The queue does not exist, creating it')
            queue = self.sqs.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '120'})
        self.queue_url=queue['QueueUrl']
        log.info("Retrieved queue_url: {}".format(self.queue_url))
        
    def pull(self,n=1):
        super().pull(n)
        log.info("Polling request from queue {}...".format(self.queue_url))
        response = self.sqs.receive_message(QueueUrl=self.queue_url,WaitTimeSeconds=config.SQS_WAIT_TIME_SEC)
        while 'Messages' not in response:
            log.info("Polling request from queue {}...".format(self.queue_url))
            response = self.sqs.receive_message(QueueUrl=self.queue_url,WaitTimeSeconds=config.SQS_WAIT_TIME_SEC)
        log.info('response: ' + str(response))
        requests=[]
        for m in response['Messages']:
            log.info(str(m))
            log.info('m.body: ' + str(m['Body']))
            requests.append(Request(request=m['Body'],request_id=m['ReceiptHandle']))
        return requests

    def push(self,messages):
        for m in messages:
            response = self.sqs.send_message(QueueUrl=self.queue_url,MessageBody=m)
            
    def delete_message(self, request_id):
        """Delete a message from the SQS queue given a request_id

        Args:
            request_id: request_id from sqs.receive_message
        """
        self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=request_id)