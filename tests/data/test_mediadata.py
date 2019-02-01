import json

import boto3

from vitamincv.data import create_detection
from vitamincv.data.avro_response import AvroResponse


TEST_BUCKET = "vitamin-cv-test-data"
TEST_DATA = "data/previous_responses/short_flamesatblues.json"

def init():
    global response
    s3client = boto3.client('s3')
    obj = s3client.get_object(Bucket=TEST_BUCKET, Key=TEST_DATA)
    j = json.loads(obj['Body'].read())
    response = AvroResponse(j)

def test_create_detections():
    init()
    prop1 = {"server": "NHLLogoClassifier"}
    prop2 = {"value": "AAA"}
    pois = [prop1, prop2]

    md = response.to_mediadata(pois)
    dets = md.detections
    # md.filter_detections_by_props(prop)
    print(f'dets {len(dets)}')

    for prop in pois:
        for k, v in prop.items():
            dets = list(filter(lambda det: det.get(k) == v, dets))
    print(f'dets {len(dets)}')

