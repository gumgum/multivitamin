import context
import sys
import os

import glog as log
import requests
import zipfile2
import boto3
from time import sleep

sys.path.append(os.path.abspath("../examples/ssd-caffe/"))
sys.path.append(os.path.abspath("../examples/"))
from dummy_detector import DummyDetector
from ssd_detector import SSDDetector
from multivitamin.module_api.server import Server
from multivitamin.comm_apis.sqs_api import SQSAPI
from multivitamin.comm_apis.vertex_api import VertexAPI
from multivitamin.comm_apis.local_api import LocalAPI

PORT = os.environ.get("PORT", 5000)

request1 = '{"url":"https://www.astrosafari.com/download/file.php?id=1608&sid=79f086b975b702945ca90b0ac3bd0202","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=5&moduleId=1&imageUrl\
=https%3A%2F%2Fwww.astrosafari.com%2Fdownload%2Ffile.php%3Fid%3D1608%26sid%3D79f086b975b702945ca90b0ac3bd0202&order=2&async=true","prev_response":"AAAAAEAGMi4wJjIwMTgtMTAtMTZUMTc6MjM6MjAAAAAAAAAAAAAAAhxOU0\
ZXQ2xhc3NpZmllcgAKNC4wLjEOU1VDQ0VTUwxndW1ndW0cMjAxODEwMTYxNzIzMjAObWFjaGluZQAAOk5TRldDbGFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwALwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD0\
3OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMrwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD03OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMgCACsAHAgAAAAAChgEwLjAwMDBfKDAuMDAw\
MCwwLjAwMDApKDEuMDAwMCwwLjAwMDApKDEuMDAwMCwxLjAwMDApKDAuMDAwMCwxLjAwMDApAAgAAAAAAAAAAAAAgD8AAAAAAACAPwAAgD8AAAAAAACAPwACEgIcTlNGV0NsYXNzaWZpZXIACjQuMC4xDGd1bWd1bQxzYWZldHkIc2FmZQAc1nk/CtcjPAAAgD8AOk5TRldDb\
GFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwAAAAAAIAAAAAAAAAAAISAhxOU0ZXQ2xhc3NpZmllchJoaXN0b2dyYW0KNC4wLjEMZ3VtZ3VtDHNhZmV0eQhzYWZlABzWeT8K1yM8AACAPwA6TlNGV0NsYXNzaWZpZXJfMjAxODEwMTYxNzIzMjAAAAAAAAA=","bin_encoding":"\
true","bin_decoding":"true"}'
request2 = '{"url":"https://s3.amazonaws.com/cvapis-media/images/Hackathon-Group.jpg","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=41&moduleId=1000&imageUrl=https://s3.amazonaws.com/cvapis-media/images/Hackathon-Group.jpg&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'
request3 = '{"url":"https://s3.amazonaws.com/cvapis-media/images/Hackathon-Group.jpg","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=26&moduleId=1004&imageUrl=https://s3.amazonaws.com/cvapis-media/images/Hackathon-Group.jpg&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'

message = ""

NET_DATA_DIR = "/tmp/net_data"
SERVER_NAME = "ObjectDetector"
VERSION = "1.0.0"
log.setLevel("INFO")


def pull_net_data():
    log.info("Downloading net_data")
    s3_client = boto3.client("s3")
    log.info("Downloading model.")
    tmp_filepath = "/tmp/net_data.zip"
    s3_client.download_file("cvapis-data", "ssd-detector/net_data.zip", tmp_filepath)
    log.info("Model downloaded.")
    with open(tmp_filepath, "rb") as f:
        log.info("Unzipping it.")
        z = zipfile2.ZipFile(f)
        for name in z.namelist():
            print("    Extracting file", name)
            z.extract(name, "/tmp/")
    log.info("Unzipped.")


def test_server_vertex_ssddetector():
    log.info("Testing a server with SSDDetector and SQSAPI")
    queue_name = "cvapis-testing"
    log.info("Sending test requests to queue named " + queue_name)
    sqs_api = SQSAPI(queue_name=queue_name)
    sqs_api.push([request1])
    sqs_api.push([request2])
    sqs_api.push([request3])
    log.info("Creating VertexAPI")
    vertex_api = VertexAPI(queue_name=queue_name)

    # if not os.path.exists(NET_DATA_DIR):
    if True:
        pull_net_data()

    ssd_detector = SSDDetector(SERVER_NAME, VERSION, net_data_dir=NET_DATA_DIR)

    server = Server(ssd_detector, vertex_api)
    log.info("Starting server")
    log.info("******************")
    log.info("******************")
    log.info("******************")
    server.start()


def test_server_localapi_ssddetector():
    log.info("Testing a server with SSDDetector and LocalAPI")

    pulling_folder = "data/requests"
    pushing_folder = "data/output"
    log.info("Folder of requests to process: {}".format(pulling_folder))

    # if not os.path.exists(NET_DATA_DIR):
    if True:
        pull_net_data()

    ssd_detector = SSDDetector(SERVER_NAME, VERSION, NET_DATA_DIR)

    local_api = LocalAPI(pulling_folder, pushing_folder, True)
    server = Server(ssd_detector, local_api)
    log.info("Starting server")
    server.start()


def test_vertexpoller_to_local_server():
    log.info("Testing a dummy server polling from a queue and dumping with LocalAPI")
    pushing_folder = "data/output_nhl_historical"
    dummy_detector = DummyDetector("dummy", "1.0", None)
    sqs_api = SQSAPI(queue_name="SPORTS-FinalResponses-DEV")
    local_api = LocalAPI(pulling_folder=None, pushing_folder=pushing_folder)
    server = Server(dummy_detector, sqs_api, [local_api])
    log.info("Starting server")
    server.start()


def test_health_endpoint():
    log.info("Testing a dummy server and querying the endpoint")
    dummy_detector = DummyDetector("dummy", "1.0", None)
    local_api = LocalAPI(pulling_folder=None, pushing_folder="output")
    server = Server(dummy_detector, local_api)
    log.info("Starting server")
    server.start()
    sleep(1)  # wait for server to start
    url = "http://0.0.0.0:{}/health".format(PORT)
    log.info("HTTP get on {}".format(url))
    res = requests.get(url)
    log.info("response.text: {}".format(res.text))
    assert res.status_code == 200
    assert res.text
