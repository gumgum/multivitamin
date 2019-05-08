import os
import sys
import glog as log
import boto3
import json
<<<<<<< HEAD
from multivitamin.avro_api.frame_drawer import FrameDrawer
from multivitamin.avro_api.avro_api import AvroIO, AvroAPI

S3_BUCKET = "cv-applications-responses-staging"
# JSON_NAME="Replay%20Video%20Capture_2018-11-16_11.52.51-2816an1tb0v"
# JSON_NAME="538_Pelicans+vs+Thunder+11%3A5-fhj713lbrhi.30-31"
JSON_NAME = "1%3A3%20Houston%20Rockets%20at%20Golden%20State%20Warriors-6tgm4my1dr6"
S3_KEY = "SPORTS-NBALeagueDetector_1.0.0/" + JSON_NAME + ".json"


def test_FrameDrawer():
    S3 = boto3.resource("s3")
    file = S3.Object(S3_BUCKET, S3_KEY)
    AVRO_JSON = json.loads(file.get()["Body"].read().decode("utf-8"))
    output_folder = "./frame_visualization/" + JSON_NAME
    try:
        os.makedirs(output_folder)
    except:
        pass
    avro_api = AvroAPI(doc=AVRO_JSON)
    fd = FrameDrawer(avro_api=avro_api, decode=False, dump=True, out=output_folder)
    fd.process()
=======
from vitamincv.applications.general.frame_drawer import FrameDrawer
from vitamincv.avro_api.avro_api import AvroIO, AvroAPI

S3_BUCKET = "vitamincv-data"
JSON_NAME_1="Winnipeg%2520Jets%2520%2540%2520St.%2520Louis%2520Blues-uvr287fay9k-1min"
S3_KEY_1 = "jsons/" + JSON_NAME_1 + ".json"


def test_FrameDrawer():
    S3 = boto3.resource('s3')
    file = S3.Object(S3_BUCKET, S3_KEY_1)
    AVRO_JSON = json.loads(file.get()['Body'].read().decode('utf-8'))
    output_folder='./frame_drawer_output/'  
    avro_api=AvroAPI(doc=AVRO_JSON )
    fd = FrameDrawer(avro_api=avro_api,pushing_folder=output_folder,s3_bucket='vitamincv-data',s3_key='frame_drawer')    
    fd.process(dump_video=True)
def test_FrameDrawer_B():
    S3 = boto3.resource('s3')
    file = S3.Object(S3_BUCKET, S3_KEY_1)
    AVRO_JSON = json.loads(file.get()['Body'].read().decode('utf-8'))
    output_folder='./frame_drawer_output/'
    avro_api=AvroAPI(doc=AVRO_JSON )
    fd = FrameDrawer(avro_api=avro_api,s3_bucket='vitamincv-data',s3_key='frame_drawer')    
    fd.process(dump_images=True)
>>>>>>> origin/develop
