import sys
import os
import json
import glog as log
from multivitamin.applications.classifiers.caffe_classifier import CaffeClassifier
import boto3
import zipfile

# request1='["com.amazon.sqs.javamessaging.MessageS3Pointer",{"s3BucketName":"cvapis-data","s3Key":"f2d9a5d0-06e6-4975-8889-3fe79508972b"}]'
request1 = '["com.amazon.sqs.javamessaging.MessageS3Pointer",{"s3BucketName":"cvapis-data","s3Key":"1be661e2-4190-4d05-a4a4-156ddf9967fc"}]'


message = ""


def test_classifier():
    log.info("Testing caffe classifier.")

    log.info("Downloading net_data")

    s3_client = boto3.client("s3")
    log.info("Downloading model.")
    tmp_filepath = "/tmp/net_data.zip"
    s3_client.download_file(
        "cvapis-data", "classifiers/nhllogoclassifier/net_data.zip", tmp_filepath
    )
    log.info("Model downloaded.")
    with open(tmp_filepath, "rb") as f:
        log.info("Unzipping it.")
        z = zipfile.ZipFile(f)
        for name in z.namelist():
            print("    Extracting file", name)
            z.extract(name, "/tmp/")
    log.info("Unzipped.")
    caffe_classifier = CaffeClassifier(
        server_name="CaffeClassifier", version="1.0", net_data_dir="/tmp/net_data/"
    )
    p = {}
    p["company"] = "gumgum"
    # p['property_type']='placement'
    caffe_classifier.set_prev_pois(prev_pois=[p])

    log.info("1---------------------")
    caffe_classifier.process(request1)
    caffe_classifier.update_response()
    log.info("Getting response")
    try:
        response = caffe_classifier.request_api.get_response()
    except Exception as e:
        # log.error(e)
        log.error("Problem retrieving response")
    log.info("Jsonifying response")
    response_json = json.dumps(response, indent=4)
    log.info("---------------------")
    log.info("---------------------")
