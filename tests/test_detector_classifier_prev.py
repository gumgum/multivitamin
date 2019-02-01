"""Test a 2 module sequence on a short video of cars

1) object detector
2) make-model classifier after filtering only for car detections
"""

import os
import sys
import json

import glog as log
import requests
import boto3
import zipfile
from vitamincv.comm_apis.request_api import RequestAPI
from vitamincv.applications.detectors.ssd_detector import SSDDetector
from vitamincv.applications.classifiers.caffe_classifier import CaffeClassifier


test_video = "https://s3.amazonaws.com/video-ann-testing/kitti-clip.mp4"
dst_video = os.path.join("/tmp", os.path.basename(test_video))
log.setLevel("DEBUG")


def test_app():
    _download_video()
    _download_obj_det()
    _download_clf()
    req = {"url": test_video, "sample_rate": 5.0}
    req = RequestAPI(req)
    log.info("Processing request: {}".format(req))
    ssd = SSDDetector(server_name="ObjectDetector", version="1.0", net_data_dir="/tmp/ssd/net_data/")
    resp = ssd.process(req)
    assert resp == "SUCCESS"
    ssd.update_response()

    req.reset_media_api()
    clf = CaffeClassifier(server_name="MakeModelClassifier", version="1.0", net_data_dir="/tmp/clf/net_data")
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
    s3_client.download_file("cvapis-data", "ssd-detector/objectdetector/net_data.zip", dl_file)
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
    s3_client.download_file("cvapis-data", "classifiers/makemodelclassifier/net_data.zip", dl_file)
    log.info("Model downloaded")
    with open(dl_file, "rb") as f:
        log.info("Unzipping")
        z = zipfile.ZipFile(f)
        for name in z.namelist():
            z.extract(name, "/tmp/clf/")
    log.info("Unzipping... Done")
