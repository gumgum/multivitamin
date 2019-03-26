"""
example usage:
```
curl -d '{"url":"https://s3.amazonaws.com/vitamin-cv-test-data/data/media/images/nhl_image.jpg", "bin_encoding": "false"}' -H "Content-Type: application/json" -X POST http://localhost:8888/process
```
"""
import os

import boto3
import glog as log

from io import BytesIO
from multivitamin.http_server import HTTPServer
from multivitamin.applications.classifiers import CaffeClassifier


def load_fileobj(s3_bucket_name, key):
    s3_client = boto3.client("s3")
    bytes_obj = BytesIO()
    s3_client.download_fileobj(s3_bucket_name, key, bytes_obj)
    return bytes_obj


def generate_fileobj_from_s3_folder(s3_bucket_name, s3_folder_key):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(name=s3_bucket_name)
    for obj in s3_bucket.objects.filter(Prefix=s3_folder_key):
        if obj.key == s3_folder_key:
            continue
        bytes_obj = load_fileobj(obj.bucket_name, obj.key)
        yield obj.key, bytes_obj


S3_BUCKET_NAME = "vitamin-cv-test-data"
S3_IMAGES_FOLDER = "data/media/images/generic/"
S3_EXPECTED_PREV_RESPONSES = "data/previous_responses"
S3_PREV_DETECTIONS_FOLDER = "data/sample_detections/random_boxes/"
S3_NET_DATA_FOLDER = "models/caffe/bvlc/SportsNHLLogoClassifier-v1.2/"  # DO NOT TOUCH

LOCAL_NET_DATA_DIR = "/tmp/test_caffe_classifier/net_data"


server_name = "TestClassifier"
version = "0.0.0"
cc = None
if os.path.exists(LOCAL_NET_DATA_DIR):
    try:
        cc = CaffeClassifier(server_name, version, LOCAL_NET_DATA_DIR)
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

httpserver = HTTPServer([cc])
httpserver.start()
