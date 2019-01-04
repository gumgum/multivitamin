import boto3
import cv2
import json

import numpy as np

from utils import generate_fileobj_from_s3_folder

from cvapis.applications.classifiers.caffe_classifier import CaffeClassfier


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

    if not os.path.exists(net_data_dir):
        os.makedirs(net_data_dir)
        for key, net_data_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_NET_DATA_FOLDER):
            filename = os.path.basename(key)
            with open("{}/{}".format(net_data_dir, filename), "wb") as file:
                file.write(net_data.getvalue())

    cc = CaffeClassfier(server_name, version, net_data_dir)

def test_preprocess_images():
    global images_, images_cropped

    # Load Batch of Images
    images = []
    for _, im_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_IMAGES_FOLDER):
        im_flat = np.fromstring(im_bytes.getvalue())
        images.append(cv2.imdecode(im_flat, cv2.IMREAD_COLOR))

    sample_prev_detections = []
    for _, det_bytes in generate_fileobj_from_s3_folder(S3_BUCKET_NAME, S3_PREV_DETECTIONS_FOLDER):
        sample_prev_detections.append(json.loads(det_bytes.getvalue().decode("utf-8")))

    assert(len(images) == len(sample_prev_detections))

    # Test preprocessing
    images_ = cc.preprocess_images(images)
    assert(len(images) == images_.shape[0])

    images_cropped = cc.preprocess_images(images)
    assert(images_.shape == images_cropped.shape)
    assert(images_ != images_cropped)

def test_process_images():
    global preds, preds_from_crops
    preds = cc.process_images(images_)
    preds_from_crops = cc.process_images(images_cropped, sample_prev_detections)

    assert(preds.shape == preds_from_crops.shape)
    assert(preds.shape[0] == images_.shape[0])

def test_postprocess_predictions():
    global postprocessed_preds, postprocessed_preds_from_crops
    postprocessed_preds = cc.postprocess_predictions(preds)
    postprocessed_preds_from_crops = cc.postprocess_predictions(preds_from_crops)

def test_append_detections():
    current_detections = cc.detections.copy()
    
    # Vanilla Append
    cc.append_detections(postprocessed_preds)
    cc.detections = current_detections.copy()

    cc.append_detections(postprocessed_preds_from_crops)
    cc.detections = current_detections.copy()

    # Append with Tstamps

    # Append with previous_detections

    # Append with both


def test_process():
    # Test on short video
    # message = None
    # cc.process(message)
    pass