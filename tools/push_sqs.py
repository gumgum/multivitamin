import sys, os
import argparse
from glob import glob
import boto3

sqs = boto3.resource('sqs')
a = argparse.ArgumentParser()
a.add_argument("--queue_name")
a.add_argument("--requests_dir")
args = a.parse_args()

#reqs = []
#with open(args.request_txt) as rf:
#    reqs = rf.readlines()

#reqs = [r.strip() for r in reqs]
queue = sqs.get_queue_by_name(QueueName=args.queue_name)
#for req in reqs:
for req in glob(os.path.join(args.requests_dir, "*")):
    with open(req) as rf:
        resp = queue.send_message(MessageBody=rf.read().strip())
        print(resp)
    
