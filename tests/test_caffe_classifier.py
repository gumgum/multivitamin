import boto3
import cv2
import json
import os
from io import BytesIO

import numpy as np

from utils import generate_fileobj_from_s3_folder

from vitamincv.applications.classifiers.caffe_classifier import CaffeClassifier
from vitamincv.module_api.utils import load_idmap


S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_IMAGES_FOLDER = "data/media/images/generic/"
S3_EXPECTED_PREV_RESPONSES = "data/previous_responses"
S3_PREV_DETECTIONS_FOLDER = "data/sample_detections/random_boxes/"
S3_NET_DATA_FOLDER = "models/caffe/bvlc/SportsNHLLogoClassifier-v1.2/" # DO NOT TOUCH

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
        except:
            pass
    else:
        os.makedirs(LOCAL_NET_DATA_DIR)

    for key, net_data_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_NET_DATA_FOLDER):
        filename = os.path.basename(key)
        print("{}/{}".format(LOCAL_NET_DATA_DIR, filename))
        with open("{}/{}".format(LOCAL_NET_DATA_DIR, filename), "wb") as file:
            file.write(net_data_bytes.getvalue())

    cc = CaffeClassifier(server_name, version, LOCAL_NET_DATA_DIR)

def test_preprocess_images():
    global images_, images_cropped, sample_prev_detections

    # Load Batch of Images
    images = []
    for _, im_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_IMAGES_FOLDER):
        _ = im_bytes.seek(0)
        im_flat = np.frombuffer(im_bytes.read(), np.uint8)
        images.append(cv2.imdecode(im_flat, -1))

    sample_prev_detections = []
    for _, det_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_PREV_DETECTIONS_FOLDER):
        sample_prev_detections.append(json.loads(det_bytes.getvalue().decode("utf-8")))

    max_len = min(len(images), len(sample_prev_detections))
    images = images[:max_len]
    sample_prev_detections = sample_prev_detections[:max_len]
    assert(len(images) == len(sample_prev_detections))

    # Test preprocessing
    images_ = cc.preprocess_images(images)
    assert(len(images) == images_.shape[0])

    images_cropped = cc.preprocess_images(images, sample_prev_detections)
    assert(images_.shape == images_cropped.shape)
    assert(not np.array_equal(images_, images_cropped))

def test_process_images():
    global preds, preds_from_crops
    preds = cc.process_images(images_)
    preds_from_crops = cc.process_images(images_cropped)

    assert(preds.shape == preds_from_crops.shape)
    assert(preds.shape[0] == images_.shape[0])
    assert(not np.array_equal(preds, preds_from_crops))

def test_postprocess_predictions():
    global postprocessed_preds, postprocessed_preds_from_crops
    postprocessed_preds = cc.postprocess_predictions(preds)
    postprocessed_preds_from_crops = cc.postprocess_predictions(preds_from_crops)

def test_convert_to_detections():
    global postprocessed_preds, postprocessed_preds_from_crops
    current_detections = cc.detections.copy()
    tstamps = [det["t"] for det in sample_prev_detections]
    # DO STUFF

def test_process():
    # Test on short video
    message = {
        "url":"https://s3.amazonaws.com/vitamin-cv-test-data/data/media/videos/generic-short/kitti-clip.mp4"
    }
    cc.process(message)

def download_expected_response(path):
    s3 = boto3.client("s3")
    filelike = BytesIO()
    s3.download_fileobj(S3_BUCKET_NAME, S3_EXPECTED_PREV_RESPONSES+"/"+path, filelike)
    filelike.seek(0)
    expected_json = json.loads(filelike.read().decode())
    return expected_json

def test_consistency1():
    # Just an image
    pass

def test_consistency2():
    #  Just a video
    expected_json = download_expected_response("classfiication_doc_without_prev_resp.json")

    message = {
        "url":expected_json["media_annotation"]["url"]
    }
    module_map ={}
    module_map['NHLPlacementDetector'] =60
    module_map['NHLLogoClassifier']=61
    module_map['NBAPlacementDetector'] =69
    module_map['NBALogoClassifier']=70
    module_map['NBALeagueDetector']=71

    sponsor_id_map_file= LOCAL_NET_DATA_DIR+"/idmap.txt"
    sponsor_map=load_idmap(sponsor_id_map_file)

    cc = CaffeClassifier("NHLLogoClassifier", "0.0.2", LOCAL_NET_DATA_DIR,prop_type="logo",prop_id_map=sponsor_map,module_id_map=module_map)
    cc.process(message)
    cc.update_response()
    j1 = json.dumps(expected_json, indent=2, sort_keys=True)
    j2 = json.dumps(cc.avro_api.doc, indent=2, sort_keys=True)
    j2 = j2.replace(cc.avro_api.doc["media_annotation"]["codes"][0]["date"], expected_json["media_annotation"]["codes"][0]["date"])
    j2 = j2.replace(cc.avro_api.doc["media_annotation"]["codes"][0]["id"], expected_json["media_annotation"]["codes"][0]["id"])
    print(j2)
    assert(j1 == j2)
    assert(json.loads(j1) == json.loads(j2))
    assert(cc.avro_api.doc == expected_json)

def test_consistency3():
    # A video with previous response
    expected_json = download_expected_response("classfiication_doc_with_prev_resp.json")
    prev_resp = download_expected_response("detection_doc.json")

    message = {
        "url":expected_json["media_annotation"]["url"],
        "prev_response":json.dumps(prev_resp)
    }
    module_map ={}
    module_map['NHLPlacementDetector'] =60
    module_map['NHLLogoClassifier']=61
    module_map['NBAPlacementDetector'] =69
    module_map['NBALogoClassifier']=70
    module_map['NBALeagueDetector']=71

    sponsor_id_map_file= LOCAL_NET_DATA_DIR+"/idmap.txt"
    sponsor_map=load_idmap(sponsor_id_map_file)

    cc = CaffeClassifier("NHLLogoClassifier", "0.0.2", LOCAL_NET_DATA_DIR,prop_type="logo",prop_id_map=sponsor_map,module_id_map=module_map)
    cc.process(message)
    cc.update_response()
    assert(cc.avro_api.doc == expected_json)
