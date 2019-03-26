import os
import json

import glog as log
import numpy as np
import boto3
import cv2

from multivitamin.applications.detectors.ssd_detector import SSDDetector
from multivitamin.data.request import Request
from multivitamin.data.response import Response
from multivitamin.data.response.io import load_response_from_request
from utils import generate_fileobj_from_s3_folder

S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_IMAGES_FOLDER = "data/media/images/generic/"
S3_PREV_DETECTIONS_FOLDER = "data/sample_detections/random_boxes/"
S3_NET_DATA_FOLDER = "models/caffe/ssd/SportsNHLPlacementDetector-v1.2/"

import zipfile

request1 = '{"url":"https://www.astrosafari.com/download/file.php?id=1608&sid=79f086b975b702945ca90b0ac3bd0202","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=5&moduleId=1&imageUrl\
=https%3A%2F%2Fwww.astrosafari.com%2Fdownload%2Ffile.php%3Fid%3D1608%26sid%3D79f086b975b702945ca90b0ac3bd0202&order=2&async=true","prev_response":"AAAAAEAGMi4wJjIwMTgtMTAtMTZUMTc6MjM6MjAAAAAAAAAAAAAAAhxOU0\
ZXQ2xhc3NpZmllcgAKNC4wLjEOU1VDQ0VTUwxndW1ndW0cMjAxODEwMTYxNzIzMjAObWFjaGluZQAAOk5TRldDbGFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwALwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD0\
3OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMrwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD03OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMgCACsAHAgAAAAAChgEwLjAwMDBfKDAuMDAw\
MCwwLjAwMDApKDEuMDAwMCwwLjAwMDApKDEuMDAwMCwxLjAwMDApKDAuMDAwMCwxLjAwMDApAAgAAAAAAAAAAAAAgD8AAAAAAACAPwAAgD8AAAAAAACAPwACEgIcTlNGV0NsYXNzaWZpZXIACjQuMC4xDGd1bWd1bQxzYWZldHkIc2FmZQAc1nk/CtcjPAAAgD8AOk5TRldDb\
GFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwAAAAAAIAAAAAAAAAAAISAhxOU0ZXQ2xhc3NpZmllchJoaXN0b2dyYW0KNC4wLjEMZ3VtZ3VtDHNhZmV0eQhzYWZlABzWeT8K1yM8AACAPwA6TlNGV0NsYXNzaWZpZXJfMjAxODEwMTYxNzIzMjAAAAAAAAA=","bin_encoding":"\
true","bin_decoding":"true"}'
request2 = '{"url":"http://pbs.twimg.com/media/Dpq4c0OXcAII7J2.jpg:large","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=41&moduleId=1000&imageUrl=http%3A%2F%2Fpbs.twimg.com%2Fmedia%2FDpq4c0OXcAII7J2.jpg%3Alarge&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'
request3 = '{"url":"http://pbs.twimg.com/media/Dpq40egWkAAtocH.jpg:large","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=26&moduleId=1004&imageUrl=http%3A%2F%2Fpbs.twimg.com%2Fmedia%2FDpq40egWkAAtocH.jpg%3Alarge&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'

message = ""


def test_object_detector():
    log.info("Testing SSD detector.")
    log.info("Downloading net_data")
    s3_client = boto3.client("s3")
    log.info("Downloading model.")
    tmp_filepath = "/tmp/net_data.zip"
    s3_client.download_file(
        "cvapis-data",
        "ssd-detector/nhlplacementdetector/net_data_v1.1.zip",
        tmp_filepath,
    )
    log.info("Model downloaded.")
    with open(tmp_filepath, "rb") as f:
        log.info("Unzipping it.")
        z = zipfile.ZipFile(f)
        for name in z.namelist():
            print("    Extracting file", name)
            z.extract(name, "/tmp/")
    log.info("Unzipped.")
    ssd_detector = SSDDetector(
        server_name="SSDDetector", version="1.0", net_data_dir="/tmp/net_data/"
    )
    log.info("1---------------------")
    ssd_detector.process(request1)
    log.info("2---------------------")
    ssd_detector.process(request2)
    log.info("3---------------------")
    ssd_detector.process(request3)
    ssd_detector.update_response()
    log.info("---------------------")
    log.info("---------------------")


#############################
### START OF REAL TESTING ###
#############################

import boto3
import json
import os
from io import BytesIO

from utils import generate_fileobj_from_s3_folder

from multivitamin.applications.detectors import SSDDetector
from multivitamin.module_api.utils import load_idmap


S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_EXPECTED_PREV_RESPONSES = "data/previous_responses"
S3_NET_DATA_FOLDER = "models/caffe/ssd/SportsNHLPlacementDetector-v1.2/"  # DO NOT TOUCH

LOCAL_NET_DATA_DIR = "/tmp/test_caffe_detector/net_data"


def test_init():
    global cc

    server_name = "TestDetector"
    version = "0.0.0"
    # net_data_dir = "/tmp/test_caffe_classifier/net_data"
    # prop_type = "label"
    # prop_id_map = {}
    # module_id_map
    if os.path.exists(LOCAL_NET_DATA_DIR):
        try:
            cc = CaffeClassifier(server_name, version, LOCAL_NET_DATA_DIR)
            return
        except:
            pass
    else:
        os.makedirs(LOCAL_NET_DATA_DIR)

    for key, net_data_bytes in generate_fileobj_from_s3_folder(
        S3_BUCKET_NAME, S3_NET_DATA_FOLDER
    ):
        filename = os.path.basename(key)
        print("{}/{}".format(LOCAL_NET_DATA_DIR, filename))
        with open("{}/{}".format(LOCAL_NET_DATA_DIR, filename), "wb") as file:
            file.write(net_data_bytes.getvalue())

    ssd = SSDDetector(server_name, version, LOCAL_NET_DATA_DIR)


def download_expected_response(path):
    s3 = boto3.client("s3")
    filelike = BytesIO()
    s3.download_fileobj(
        S3_BUCKET_NAME, S3_EXPECTED_PREV_RESPONSES + "/" + path, filelike
    )
    filelike.seek(0)
    expected_json = json.loads(filelike.read().decode())
    return expected_json


def test_consistency1():
    # Just an image
    pass


def test_consistency2():
    #  Just a video
    expected_json = download_expected_response("detection_doc.json")

    message = {"url": expected_json["media_annotation"]["url"]}
    module_map = {}
    module_map["NHLPlacementDetector"] = 60
    module_map["NHLLogoClassifier"] = 61
    module_map["NBAPlacementDetector"] = 69
    module_map["NBALogoClassifier"] = 70
    module_map["NBALeagueDetector"] = 71

    placement_id_map_file = LOCAL_NET_DATA_DIR + "/idmap.txt"
    placement_map = load_idmap(placement_id_map_file)

    ssd = SSDDetector(
        "NHLPlacementDetector",
        "0.0.1",
        LOCAL_NET_DATA_DIR,
        prop_type="placement",
        prop_id_map=placement_map,
        module_id_map=module_map,
    )
    ssd.process(message)
    ssd.update_response()
    j1 = json.dumps(expected_json, indent=2, sort_keys=True)
    j2 = json.dumps(ssd.avro_api.doc, indent=2, sort_keys=True)
    j2 = j2.replace(
        ssd.avro_api.doc["media_annotation"]["codes"][0]["date"],
        expected_json["media_annotation"]["codes"][0]["date"],
    )
    j2 = j2.replace(
        ssd.avro_api.doc["media_annotation"]["codes"][0]["id"],
        expected_json["media_annotation"]["codes"][0]["id"],
    )
    assert j1 == j2
    assert json.loads(j1) == json.loads(j2)
