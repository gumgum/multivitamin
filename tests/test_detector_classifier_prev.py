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

from multivitamin.applications.detectors import SSDDetector
from multivitamin.applications.classifiers import CaffeClassifier
from multivitamin.data.request import Request
from multivitamin.data.response import Response
from multivitamin.data.response.io import load_response_from_request
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

        for key, net_data_bytes in generate_fileobj_from_s3_folder(
            S3_BUCKET_NAME, SSD_S3_NET_DATA_FOLDER
        ):
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

        for key, net_data_bytes in generate_fileobj_from_s3_folder(
            S3_BUCKET_NAME, CAFFE_S3_NET_DATA_FOLDER
        ):
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
    message = {
        "url": "https://s3.amazonaws.com/video-ann-testing/short_flamesatblues.mp4",
        "bin_encoding": "false",
        "bin_decoding": "false",
    }
    request = Request(message)
    response = load_response_from_request(request)
    log.info("SSD DETECTOR")
    response = ssd.process(request, response)
    log.info("CAFFE CLASSIFIER")
    cc.set_prev_props_of_interest(
        [
            {
                "property_type": "placement",
                "company": "gumgum",
                "server": "TestSSDClassifier",
                "value": "Static Dasherboard",
            }
        ]
    )
    response = cc.process(request, response)

    doc = response.dict
    log.info(f"doc: {json.dumps(doc, indent=2)}")
    with open("t.json", "w") as wf:
        json.dump(doc, wf)


test_video = "https://s3.amazonaws.com/video-ann-testing/kitti-clip.mp4"
dst_video = os.path.join("/tmp", os.path.basename(test_video))
# log.setLevel("DEBUG")


def test_app():
    _download_video()
    _download_obj_det()
    _download_clf()
    req = {"url": test_video, "sample_rate": 5.0}
    req = RequestAPI(req)
    log.info("Processing request: {}".format(req))
    ssd = SSDDetector(
        server_name="ObjectDetector", version="1.0", net_data_dir="/tmp/ssd/net_data/"
    )
    resp = ssd.process(req)
    assert resp == "SUCCESS"
    ssd.update_response()

    req.reset_media_api()
    clf = CaffeClassifier(
        server_name="MakeModelClassifier",
        version="1.0",
        net_data_dir="/tmp/clf/net_data",
    )
    clf.set_prev_pois({"value": "car"})
    resp = clf.process(req)
    assert resp == "SUCCESS"
    out = req.get_avro_api().get_response()
    print(json.dumps(out, indent=2))


def _download_video():
    log.info("Downloading video")
    r = requests.get(test_video)
    log.info("status code: {}".format(r.status_code))
    assert r.status_code == 200
    with open(dst_video, "wb") as f:
        f.write(r.content)
    assert os.path.exists(dst_video)
    log.info("{}".format(dst_video))


def _download_obj_det():
    log.info("Downloading object detector model")
    s3_client = boto3.client("s3")
    tmp_folder = "/tmp/ssd"
    dl_file = "{}/net_data.zip".format(tmp_folder)
    if not os.path.exists(tmp_folder):
        os.makedirs(tmp_folder)
    s3_client.download_file(
        "cvapis-data", "ssd-detector/objectdetector/net_data.zip", dl_file
    )
    log.info("Model downloaded")
    with open(dl_file, "rb") as f:
        log.info("Unzipping")
        z = zipfile.ZipFile(f)
        for name in z.namelist():
            z.extract(name, "/tmp/ssd/")
    log.info("Unzipping... Done")


def _download_clf():
    log.info("Downloading classification model")
    s3_client = boto3.client("s3")
    tmp_folder = "/tmp/clf"
    dl_file = "{}/net_data.zip".format(tmp_folder)
    if not os.path.exists(tmp_folder):
        os.makedirs(tmp_folder)
    s3_client.download_file(
        "cvapis-data", "classifiers/makemodelclassifier/net_data.zip", dl_file
    )
    log.info("Model downloaded")
    with open(dl_file, "rb") as f:
        log.info("Unzipping")
        z = zipfile.ZipFile(f)
        for name in z.namelist():
            z.extract(name, "/tmp/clf/")
    log.info("Unzipping... Done")
