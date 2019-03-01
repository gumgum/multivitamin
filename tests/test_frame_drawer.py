import os
import sys
import glog as log
import boto3
import json
from vitamincv.avro_api.frame_drawer import FrameDrawer
from vitamincv.avro_api.avro_api import AvroIO, AvroAPI

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
