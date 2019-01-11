import sys
import os
import glog as log
from vitamincv.applications.detectors.ssd_detector import SSDDetector
import boto3
import zipfile

request1='{"url":"https://www.astrosafari.com/download/file.php?id=1608&sid=79f086b975b702945ca90b0ac3bd0202","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=5&moduleId=1&imageUrl\
=https%3A%2F%2Fwww.astrosafari.com%2Fdownload%2Ffile.php%3Fid%3D1608%26sid%3D79f086b975b702945ca90b0ac3bd0202&order=2&async=true","prev_response":"AAAAAEAGMi4wJjIwMTgtMTAtMTZUMTc6MjM6MjAAAAAAAAAAAAAAAhxOU0\
ZXQ2xhc3NpZmllcgAKNC4wLjEOU1VDQ0VTUwxndW1ndW0cMjAxODEwMTYxNzIzMjAObWFjaGluZQAAOk5TRldDbGFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwALwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD0\
3OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMrwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD03OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMgCACsAHAgAAAAAChgEwLjAwMDBfKDAuMDAw\
MCwwLjAwMDApKDEuMDAwMCwwLjAwMDApKDEuMDAwMCwxLjAwMDApKDAuMDAwMCwxLjAwMDApAAgAAAAAAAAAAAAAgD8AAAAAAACAPwAAgD8AAAAAAACAPwACEgIcTlNGV0NsYXNzaWZpZXIACjQuMC4xDGd1bWd1bQxzYWZldHkIc2FmZQAc1nk/CtcjPAAAgD8AOk5TRldDb\
GFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwAAAAAAIAAAAAAAAAAAISAhxOU0ZXQ2xhc3NpZmllchJoaXN0b2dyYW0KNC4wLjEMZ3VtZ3VtDHNhZmV0eQhzYWZlABzWeT8K1yM8AACAPwA6TlNGV0NsYXNzaWZpZXJfMjAxODEwMTYxNzIzMjAAAAAAAAA=","bin_encoding":"\
true","bin_decoding":"true"}'
request2='{"url":"http://pbs.twimg.com/media/Dpq4c0OXcAII7J2.jpg:large","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=41&moduleId=1000&imageUrl=http%3A%2F%2Fpbs.twimg.com%2Fmedia%2FDpq4c0OXcAII7J2.jpg%3Alarge&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'
request3='{"url":"http://pbs.twimg.com/media/Dpq40egWkAAtocH.jpg:large","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=26&moduleId=1004&imageUrl=http%3A%2F%2Fpbs.twimg.com%2Fmedia%2FDpq40egWkAAtocH.jpg%3Alarge&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'

message=""
def test_object_detector():
    log.info("Testing SSD detector.")
    log.info("Downloading net_data")
    s3_client = boto3.client('s3')
    log.info("Downloading model.")
    tmp_filepath='/tmp/net_data.zip'
    s3_client.download_file('vitamincv-data', 'ssd-detector/nhlplacementdetector/net_data_v1.1.zip', tmp_filepath)
    log.info("Model downloaded.")
    with open(tmp_filepath, 'rb') as f:
        log.info("Unzipping it.")    
        z = zipfile.ZipFile(f)
        for name in z.namelist():
            print("    Extracting file", name)
            z.extract(name,"/tmp/")
    log.info("Unzipped.")   
    ssd_detector=SSDDetector(server_name="SSDDetector",version="1.0",net_data_dir="/tmp/net_data/")
    log.info("1---------------------")
    ssd_detector.process(request1)
    log.info("2---------------------")
    ssd_detector.process(request2)
    log.info("3---------------------")
    ssd_detector.process(request3)
    ssd_detector.update_response()
    log.info("---------------------")
    log.info("---------------------")