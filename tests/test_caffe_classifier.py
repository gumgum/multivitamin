import json
import os

import glog as log
import numpy as np
import boto3
import cv2

from vitamincv.applications.classifiers.caffe_classifier import CaffeClassifier
from vitamincv.data.request import Request
from vitamincv.data.avro_response import AvroResponse
from utils import generate_fileobj_from_s3_folder

S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_IMAGES_FOLDER = "data/media/images/generic/"
S3_PREV_DETECTIONS_FOLDER = "data/sample_detections/random_boxes/"
S3_NET_DATA_FOLDER = "models/caffe/SportsNHLLogoClassifier-v1.2/"

def test_init():
    global cc

    server_name = "TestClassifier"
    version = "0.0.0"
    net_data_dir = "/tmp/test_caffe_classifier/net_data"
    # prop_type = "label"
    # prop_id_map = {}
    # module_id_map
    if os.path.exists(net_data_dir):
        try:
           cc = CaffeClassifier(server_name, version, net_data_dir)
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

    cc = CaffeClassifier(server_name, version, net_data_dir)

# def test_preprocess_images():
#     global images_, images_cropped, sample_prev_detections

#     # Load Batch of Images
#     images = []
#     for _, im_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_IMAGES_FOLDER):
#         _ = im_bytes.seek(0)
#         im_flat = np.frombuffer(im_bytes.read(), np.uint8)
#         images.append(cv2.imdecode(im_flat, -1))

#     sample_prev_detections = []
#     for _, det_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_PREV_DETECTIONS_FOLDER):
#         sample_prev_detections.append(json.loads(det_bytes.getvalue().decode("utf-8")))

#     max_len = min(len(images), len(sample_prev_detections))
#     images = images[:max_len]
#     sample_prev_detections = sample_prev_detections[:max_len]
#     assert(len(images) == len(sample_prev_detections))

#     # Test preprocessing
#     images_ = cc.preprocess_images(images)
#     assert(len(images) == images_.shape[0])

#     images_cropped = cc.preprocess_images(images, sample_prev_detections)
#     assert(images_.shape == images_cropped.shape)
#     assert(not np.array_equal(images_, images_cropped))

# def test_process_images():
#     global preds, preds_from_crops
#     preds = cc.process_images(images_)
#     preds_from_crops = cc.process_images(images_cropped)
    
#     assert(preds.shape == preds_from_crops.shape)
#     assert(preds.shape[0] == images_.shape[0])
#     assert(not np.array_equal(preds, preds_from_crops))

# def test_postprocess_predictions():
#     global postprocessed_preds, postprocessed_preds_from_crops
#     postprocessed_preds = cc.postprocess_predictions(preds)
#     postprocessed_preds_from_crops = cc.postprocess_predictions(preds_from_crops)

# def test_append_detections():
#     global postprocessed_preds, postprocessed_preds_from_crops
#     current_detections = cc.detections.copy()
#     tstamps = [det["t"] for det in sample_prev_detections]
    
#     # Vanilla Append
#     cc.append_detections(postprocessed_preds.copy())
#     assert(len(current_detections) < len(cc.detections))
#     cc.detections = current_detections.copy()

#     cc.append_detections(postprocessed_preds_from_crops.copy())
#     assert(len(current_detections) < len(cc.detections))
#     cc.detections = current_detections.copy()

#     # Append with Tstamps
#     cc.append_detections(postprocessed_preds.copy(), tstamps=tstamps)
#     assert(len(current_detections) < len(cc.detections))
#     cc.detections = current_detections.copy()

#     # Append with previous_detections
#     cc.append_detections(postprocessed_preds_from_crops.copy(), previous_detections=sample_prev_detections)
#     assert(len(current_detections) < len(cc.detections))
#     cc.detections = current_detections.copy()

#     # Append with both
#     cc.append_detections(postprocessed_preds_from_crops.copy(), tstamps=tstamps, previous_detections=sample_prev_detections)
#     assert(len(current_detections) < len(cc.detections))
#     cc.detections = current_detections.copy()

def test_process():
    # Test on short video
    message = {
        "url":"https://s3.amazonaws.com/video-ann-testing/kitti-clip.mp4"
    }
    request = Request(message)
    codes = cc.process(request)
    log.info(len(cc.module_data.detections))

    response = AvroResponse()
    response.mediadata_to_response(cc.module_data)
    doc = response.to_dict()
    log.info(f"doc: {json.dumps(doc, indent=2)}")