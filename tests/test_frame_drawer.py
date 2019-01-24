import os
import sys
import glog as log
import boto3
import json
from vitamincv.avro_api.frame_drawer import FrameDrawer
from vitamincv.avro_api.avro_api import AvroIO, AvroAPI

S3_BUCKET = "vitamincv-data"
#JSON_NAME="Washington_Capitals_%40_NJ_Devils_2018-10-30_08.08.06-mvxdari82qe"
JSON_NAME="Winnipeg%2520Jets%2520%2540%2520St.%2520Louis%2520Blues-uvr287fay9k-1min"
S3_KEY = "jsons/" + JSON_NAME + ".json"
def test_FrameDrawer():
    S3 = boto3.resource('s3')
    file = S3.Object(S3_BUCKET, S3_KEY)
    AVRO_JSON = json.loads(file.get()['Body'].read().decode('utf-8'))
    output_folder='/tmp/'+JSON_NAME
    avro_api=AvroAPI(doc=AVRO_JSON )
    fd = FrameDrawer(avro_api=avro_api, decode=False, dump=True, out=output_folder)
    fd.process()
