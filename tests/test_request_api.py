"""Utils methods' testing

Usage:
pytest test_request_api.py
"""

import json
import glog as log
from vitamincv.comm_apis.request_api import RequestAPI

request1 = '{"url":"https://www.astrosafari.com/download/file.php?id=1608&sid=79f086b975b702945ca90b0ac3bd0202","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=5&moduleId=1&imageUrl=https%3A%2F%2Fwww.astrosafari.com%2Fdownload%2Ffile.php%3Fid%3D1608%26sid%3D79f086b975b702945ca90b0ac3bd0202&order=2&async=true","prev_response":"AAAAAEAGMi4wJjIwMTgtMTAtMTZUMTc6MjM6MjAAAAAAAAAAAAAAAhxOU0ZXQ2xhc3NpZmllcgAKNC4wLjEOU1VDQ0VTUwxndW1ndW0cMjAxODEwMTYxNzIzMjAObWFjaGluZQAAOk5TRldDbGFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwALwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD03OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMrwBaHR0cHM6Ly93d3cuYXN0cm9zYWZhcmkuY29tL2Rvd25sb2FkL2ZpbGUucGhwP2lkPTE2MDgmYW1wO3NpZD03OWYwODZiOTc1YjcwMjk0NWNhOTBiMGFjM2JkMDIwMgCACsAHAgAAAAAChgEwLjAwMDBfKDAuMDAwMCwwLjAwMDApKDEuMDAwMCwwLjAwMDApKDEuMDAwMCwxLjAwMDApKDAuMDAwMCwxLjAwMDApAAgAAAAAAAAAAAAAgD8AAAAAAACAPwAAgD8AAAAAAACAPwACEgIcTlNGV0NsYXNzaWZpZXIACjQuMC4xDGd1bWd1bQxzYWZldHkIc2FmZQAc1nk/CtcjPAAAgD8AOk5TRldDbGFzc2lmaWVyXzIwMTgxMDE2MTcyMzIwAAAAAAIAAAAAAAAAAAISAhxOU0ZXQ2xhc3NpZmllchJoaXN0b2dyYW0KNC4wLjEMZ3VtZ3VtDHNhZmV0eQhzYWZlABzWeT8K1yM8AACAPwA6TlNGV0NsYXNzaWZpZXJfMjAxODEwMTYxNzIzMjAAAAAAAAA=","bin_encoding":"true","bin_decoding":"true"}'
request2 = '{"url":"http://pbs.twimg.com/media/Dpq4c0OXcAII7J2.jpg:large","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=41&moduleId=1000&imageUrl=http%3A%2F%2Fpbs.twimg.com%2Fmedia%2FDpq4c0OXcAII7J2.jpg%3Alarge&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'
request3 = '{"url":"http://pbs.twimg.com/media/Dpq40egWkAAtocH.jpg:large","dst_url":"https://vertex-api-v3.gumgum.com/v1/module-responses?featureId=26&moduleId=1004&imageUrl=http%3A%2F%2Fpbs.twimg.com%2Fmedia%2FDpq40egWkAAtocH.jpg%3Alarge&order=1&async=true","prev_response":"","bin_encoding":"true","bin_decoding":"true"}'

request4 = "kk:kk"


request5 = '["com.amazon.sqs.javamessaging.MessageS3Pointer",{"s3BucketName":"cvapis-data","s3Key":"f2d9a5d0-06e6-4975-8889-3fe79508972b"}]'


def test_request_api():
    r1 = RequestAPI(request1)
    for k in r1.get_keys():
        log.info(k + ": " + r1.get(k))
    k = "kk"
    log.info("1--------------------")
    log.info(k + ": " + r1.get(k))
    assert r1.get("url") == "https://www.astrosafari.com/download/file.php?id=1608&sid=79f086b975b702945ca90b0ac3bd0202"
    log.info("2--------------------")
    r2 = RequestAPI(request2)
    log.info("3--------------------")
    r3 = RequestAPI(request3)
    log.info("4--------------------")
    r4 = RequestAPI(request4)
    log.info("5--------------------")
    r5 = RequestAPI(request5)
    assert len(r5.get("url")) > 0
    log.info("--------------------")
