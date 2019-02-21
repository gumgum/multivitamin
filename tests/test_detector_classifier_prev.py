"""Test a 2 module sequence on a short video of cars

1) object detector
2) make-model classifier after filtering only for car detections


TEST WHERE THERE IS NO PREV PROPERTIES OF INTEREST
"""

import os
import sys
import json

import glog as log
import requests
import boto3
import zipfile

from vitamincv.applications.detectors.ssd_detector import SSDDetector
from vitamincv.applications.classifiers.caffe_classifier import CaffeClassifier
from vitamincv.data.request import Request
from vitamincv.data.response import Response
from vitamincv.data.response.io import load_response_from_request
from utils import generate_fileobj_from_s3_folder

S3_BUCKET_NAME = "vitamin-cv-test-data"
CAFFE_S3_NET_DATA_FOLDER = "models/caffe/SportsNHLLogoClassifier-v1.2/"
SSD_S3_NET_DATA_FOLDER = "models/caffe/ssd/SportsNHLPlacementDetector-v1.2/"

def load():

    ssd = None
    ssd_server_name = "TestSSDClassifier"
    ssd_version = "0.0.0"
    ssd_net_data_dir = "/tmp/test_ssd_detector/net_data"
    if not os.path.exists(ssd_net_data_dir):
        os.makedirs(ssd_net_data_dir)

        for key, net_data_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, SSD_S3_NET_DATA_FOLDER):
            filename = os.path.basename(key)
            log.info("{}/{}".format(ssd_net_data_dir, filename))
            with open("{}/{}".format(ssd_net_data_dir, filename), "wb") as file:
                file.write(net_data_bytes.getvalue())

    log.info("Loading SSD")
    ssd = SSDDetector(ssd_version, ssd_version, ssd_net_data_dir)

    cc = None
    server_name = "TestClassifier"
    version = "0.0.0"
    net_data_dir = "/tmp/test_caffe_classifier/net_data"
    # prop_type = "label"
    # prop_id_map = {}
    # module_id_map
    if not os.path.exists(net_data_dir):
        os.makedirs(net_data_dir)

        for key, net_data_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, CAFFE_S3_NET_DATA_FOLDER):
            filename = os.path.basename(key)
            log.info("{}/{}".format(net_data_dir, filename))
            with open("{}/{}".format(net_data_dir, filename), "wb") as file:
                file.write(net_data_bytes.getvalue())

    log.info("Loading Caffe")
    cc = CaffeClassifier(server_name, version, net_data_dir)

    return ssd, cc

def test_process():
    # Test on short video
    ssd, cc = load()
    message = {"url": "https://s3.amazonaws.com/video-ann-testing/short_flamesatblues.mp4", "bin_encoding": "false", "bin_decoding": "false"}
    request = Request(message)
    response = load_response_from_request(request)
    log.info("SSD DETECTOR")
    response = ssd.process(request, response)
    log.info("CAFFE CLASSIFIER")
    cc.set_prev_props_of_interest([
                {
                    "property_type":"placement",
                    "company":"gumgum",
                    "server": "TestSSDClassifier",
                    "value":"Static Dasherboard"
                }
            ])
    response = cc.process(request, response)

    doc = response.dictionary
    log.info(f"doc: {json.dumps(doc, indent=2)}")
    with open("t.json", "w") as wf:
        json.dump(doc, wf)
