import os
import sys
import glog as log
import boto3
import json
from vitamincv.applications.general.frame_drawer import FrameDrawer
from vitamincv.avro_api.avro_api import AvroIO, AvroAPI

S3_BUCKET = "vitamincv-data"
JSON_NAME_1="Winnipeg%2520Jets%2520%2540%2520St.%2520Louis%2520Blues-uvr287fay9k-1min"
S3_KEY_1 = "jsons/" + JSON_NAME_1 + ".json"

def test_FrameDrawer():
    S3 = boto3.resource('s3')
    file = S3.Object(S3_BUCKET, S3_KEY_1)
    AVRO_JSON = json.loads(file.get()['Body'].read().decode('utf-8'))
    output_folder='./frame_drawer_output/'+JSON_NAME_1    
    avro_api=AvroAPI(doc=AVRO_JSON )
    fd = FrameDrawer(avro_api=avro_api)    
    fd.process(dump_folder=output_folder,dump_video=True)
def test_FrameDrawer_B():
    S3 = boto3.resource('s3')
    file = S3.Object(S3_BUCKET, S3_KEY_1)
    AVRO_JSON = json.loads(file.get()['Body'].read().decode('utf-8'))
    output_folder='./frame_drawer_output/'+JSON_NAME_1    
    avro_api=AvroAPI(doc=AVRO_JSON )
    fd = FrameDrawer(avro_api=avro_api)    
    fd.process(dump_folder=output_folder,dump_images=True)
