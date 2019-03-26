import json
import os
from io import BytesIO

import glog as log
import numpy as np
import boto3
import cv2

from multivitamin.applications.classifiers import CaffeClassifier
from multivitamin.data.request import Request
from multivitamin.data.response import Response
from utils import generate_fileobj_from_s3_folder

# from multivitamin.module_api.utils import load_idmap


S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_IMAGES_FOLDER = "data/media/images/generic/"
S3_EXPECTED_PREV_RESPONSES = "data/previous_responses"
S3_PREV_DETECTIONS_FOLDER = "data/sample_detections/random_boxes/"
S3_NET_DATA_FOLDER = "models/caffe/bvlc/SportsNHLLogoClassifier-v1.2/"  # DO NOT TOUCH

LOCAL_NET_DATA_DIR = "/tmp/test_caffe_classifier/net_data"


def test_init():
    global cc

    server_name = "TestClassifier"
    version = "0.0.0"
    # net_data_dir = "/tmp/test_caffe_classifier/net_data"
    # prop_type = "label"
    # prop_id_map = {}
    # module_id_map
    if os.path.exists(LOCAL_NET_DATA_DIR):
        try:
            cc = CaffeClassifier(server_name, version, LOCAL_NET_DATA_DIR)
            return
        except Exception as e:
            log.error(e)
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

    cc = CaffeClassifier(server_name, version, LOCAL_NET_DATA_DIR)


messages = [
    {
        "url": "https://s3.amazonaws.com/vitamin-cv-test-data/data/media/videos/generic-short/kitti-clip.mp4"
    },
    {
        "url": "https://s3.amazonaws.com/vitamin-cv-test-data/data/media/images/nhl_image.jpg"
    },
]


def test_process():
    for message in messages:
        request = Request(message)
        response = Response(request=request)
        response = cc.process(response)
        log.info(json.dumps(response.dict, indent=2))


# def download_expected_response(path):
#     s3 = boto3.client("s3")
#     filelike = BytesIO()
#     s3.download_fileobj(S3_BUCKET_NAME, S3_EXPECTED_PREV_RESPONSES+"/"+path, filelike)
#     filelike.seek(0)
#     expected_json = json.loads(filelike.read().decode())
#     return expected_json

# def test_consistency1():
#     # Just an image
#     pass

# def test_consistency2():
#     #  Just a video
#     expected_json = download_expected_response("classification_doc_without_prev_resp.json")

#     message = {
#         "url":expected_json["media_annotation"]["url"]
#     }
#     module_map ={}
#     module_map['NHLPlacementDetector'] =60
#     module_map['NHLLogoClassifier']=61
#     module_map['NBAPlacementDetector'] =69
#     module_map['NBALogoClassifier']=70
#     module_map['NBALeagueDetector']=71

#     sponsor_id_map_file= LOCAL_NET_DATA_DIR+"/idmap.txt"
#     sponsor_map=load_idmap(sponsor_id_map_file)

#     cc = CaffeClassifier("NHLLogoClassifier", "0.0.2", LOCAL_NET_DATA_DIR,prop_type="logo",prop_id_map=sponsor_map,module_id_map=module_map)
#     cc.process(message)
#     cc.update_response()
#     j1 = json.dumps(expected_json, indent=2, sort_keys=True)
#     j2 = json.dumps(cc.avro_api.doc, indent=2, sort_keys=True)
#     j2 = j2.replace(cc.avro_api.doc["media_annotation"]["codes"][0]["date"], expected_json["media_annotation"]["codes"][0]["date"])
#     j2 = j2.replace(cc.avro_api.doc["media_annotation"]["codes"][0]["id"], expected_json["media_annotation"]["codes"][0]["id"])
#     assert(j1 == j2)
#     assert(json.loads(j1) == json.loads(j2))

# def test_consistency3():
#     # A video with previous response
#     expected_json = download_expected_response("classification_doc_with_prev_resp.json")
#     prev_resp = download_expected_response("detection_doc.json")

#     message = {
#         "url":expected_json["media_annotation"]["url"],
#         "prev_response":json.dumps(prev_resp),
#         "bin_decoding":False
#     }
#     module_map ={}
#     module_map['NHLPlacementDetector'] =60
#     module_map['NHLLogoClassifier']=61
#     module_map['NBAPlacementDetector'] =69
#     module_map['NBALogoClassifier']=70
#     module_map['NBALeagueDetector']=71

#     sponsor_id_map_file= LOCAL_NET_DATA_DIR+"/idmap.txt"
#     sponsor_map=load_idmap(sponsor_id_map_file)

#     p={}
#     p['server']="NHLPlacementDetector"
#     p['property_type']='placement'
#     prev_pois=[p]

#     cc = CaffeClassifier("NHLLogoClassifier", "0.0.2", LOCAL_NET_DATA_DIR,prop_type="logo",prop_id_map=sponsor_map,module_id_map=module_map)
#     cc.set_prev_pois(prev_pois=prev_pois)
#     cc.process(message)
#     cc.update_response()
#     j1 = json.dumps(expected_json, indent=2, sort_keys=True)
#     j2 = json.dumps(cc.avro_api.doc, indent=2, sort_keys=True)
#     #print(j2)
#     j2 = j2.replace(cc.avro_api.doc["media_annotation"]["codes"][1]["date"], expected_json["media_annotation"]["codes"][1]["date"])
#     j2 = j2.replace(cc.avro_api.doc["media_annotation"]["codes"][1]["id"], expected_json["media_annotation"]["codes"][1]["id"])
#     assert(j1 == j2)
#     assert(json.loads(j1) == json.loads(j2))
