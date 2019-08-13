import sys
import argparse

import boto3

sqs = boto3.resource('sqs')
a = argparse.ArgumentParser()
a.add_argument("--queue_name")
a.add_argument("--request_txt")
args = a.parse_args()

reqs = []
with open(args.request_txt) as rf:
    reqs = rf.readlines()

reqs = [r.strip() for r in reqs]
queue = sqs.get_queue_by_name(QueueName=args.queue_name)
for req in reqs:
    resp = queue.send_message(MessageBody=req)
    print(resp)
    
