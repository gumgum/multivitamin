import os
import json

import glog as log
import numpy as np
import boto3
import cv2

from vitamincv.applications.detectors.ssd_detector import SSDDetector
from vitamincv.data.request import Request
from vitamincv.data.avro_response import AvroResponse
from utils import generate_fileobj_from_s3_folder

S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_IMAGES_FOLDER = "data/media/images/generic/"
S3_PREV_DETECTIONS_FOLDER = "data/sample_detections/random_boxes/"
S3_NET_DATA_FOLDER = "models/caffe/ssd/SportsNHLPlacementDetector-v1.2/"

def test_init():
    global cc

    server_name = "TestClassifier"
    version = "0.0.0"
    net_data_dir = "/tmp/test_ssd_detector/net_data"
    # prop_type = "label"
    # prop_id_map = {}
    # module_id_map
    if os.path.exists(net_data_dir):
        try:
           cc = SSDDetector(server_name, version, net_data_dir)
           return
        except:
            pass
    else:
        os.makedirs(net_data_dir)

    for key, net_data_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_NET_DATA_FOLDER):
        filename = os.path.basename(key)
        log.info("{}/{}".format(net_data_dir, filename))
        with open("{}/{}".format(net_data_dir, filename), "wb") as file:
            file.write(net_data_bytes.getvalue())

    cc = SSDDetector(server_name, version, net_data_dir)

def test_process():
    # Test on short video
    message = {
        "url":"https://s3.amazonaws.com/video-ann-testing/short_flamesatblues.mp4"
    }
    request = Request(message)
    codes = cc.process(request)
    log.info(len(cc.module_data.detections))

    response = AvroResponse()
    response.mediadata_to_response(cc.module_data)
    doc = response.to_dict()
    log.info(f"doc: {json.dumps(doc, indent=2)}")