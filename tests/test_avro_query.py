import boto3
import json

from vitamincv.avro_api.avro_api import AvroAPI
from vitamincv.avro_api.avro_query import *


def test_load_data():
    global dets, segs
    S3_BUCKET = "vitamincv-data"
    S3_KEY = (
        "jsons/Montreal+Canadiens+%40+Toronto+Maple+Leafs+3-17_With_Human_Tracks.json"
    )

    S3 = boto3.resource("s3")
    file = S3.Object(S3_BUCKET, S3_KEY)

    AVRO_JSON = json.loads(file.get()["Body"].read().decode("utf-8"))

    avro_api = AvroAPI(AVRO_JSON)
    dets = avro_api.get_detections_from_frame_anns()
    segs = avro_api.get_segments_from_tracks_summary()


def test_querier_init():
    global det_querier, seg_querier
    det_querier = AvroQuerier()
    det_querier.load(dets)

    seg_querier = AvroQuerier()
    seg_querier.load(segs)


def test_query_server():
    q = AvroQuery()
    q.match_server("NHLLogoClassifier")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"server": "NHLLogoClassifier"}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_value():
    q = AvroQuery()
    q.match_value("GEICO")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"value": "GEICO"}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_value_verbose():
    q = AvroQuery()
    q.match_value_verbose("")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"value_verbose": ""}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_property_type():
    q = AvroQuery()
    q.match_property_type("logo")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"property_type": "logo"}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_version():
    q = AvroQuery()
    q.match_ver("1.0.8")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"ver": "1.0.8"}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_company():
    q = AvroQuery()
    q.match_company("gumgum")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"company": "gumgum"}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_footprint_id():
    q = AvroQuery()
    q.match_footprint_id("NHLLogoClassifier_20181022144641")
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"footprint_id": "NHLLogoClassifier_20181022144641"}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_region_id():
    q = AvroQuery()
    q.match_region_id(
        "32.0320_(0.2004,0.7306)(0.2553,0.7306)(0.2553,0.8424)(0.2004,0.8424)"
    )
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {
        "region_id": "32.0320_(0.2004,0.7306)(0.2553,0.7306)(0.2553,0.8424)(0.2004,0.8424)"
    }
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_t():
    q = AvroQuery()
    q.match_t(32.032)
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"t": 32.032}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_query_confidence():
    q = AvroQuery()
    q.match_confidence(0.9548)
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"confidence": 0.9548}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


# def test_query_confidence_min():
#     q = AvroQuery()
#     q.match_confidence_min(0.1)
#     num_results = len(det_querier.query(q))
#     assert(num_results > 0)

#     q = AvroQuery()
#     example = {
#         "confidence_min": 0.1
#     }
#     q.set(example)
#     assert(num_results == len(det_querier.query(q)))

#     q.set_exclude()
#     assert(len(dets)-num_results == len(det_querier.query(q)))


def test_query_property_id():
    q = AvroQuery()
    q.match_property_id(0)
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"property_id": 0}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


# def test_query_module_id():
#     q = AvroQuery()
#     q.match_module_id(0.1)
#     num_results = len(det_querier.query(q))
#     assert(num_results > 0)

#     q = AvroQuery()
#     example = {
#         "module_id": 61
#     }
#     q.set(example)
#     assert(num_results == len(det_querier.query(q)))

#     q.set_exclude()
#     assert(len(dets)-num_results == len(det_querier.query(q)))


def test_query_range():
    q = AvroQuery()
    q.set_min_confidence(0.9)
    q.set_max_confidence(1)
    num_results = len(det_querier.query(q))
    assert num_results > 0

    q = AvroQuery()
    example = {"min_confidence": 0.9, "max_confidence": 1}
    q.set(example)
    assert num_results == len(det_querier.query(q))

    q.set_exclude()
    assert len(dets) - num_results == len(det_querier.query(q))


def test_compound_query():
    q = AvroQuery()
    q.match_server("NHLLogoClassifier")
    num_results1 = len(det_querier.query(q))
    assert num_results1 > 0

    q.match_value("GEICO")
    num_results2 = len(det_querier.query(q))
    assert num_results1 > num_results2

    q.set_min_confidence(0.9)
    q.set_max_confidence(1)
    num_results3 = len(det_querier.query(q))
    assert num_results2 > num_results3

    q.set_exclude()
    assert len(dets) - num_results3 == len(det_querier.query(q))


def test_group_query():
    Q = AvroQueryBlock()
    Q.set_operation("OR")

    q1 = AvroQuery()
    q1.match_server("NHLLogoClassifier")
    q1.match_value("Lexus")
    q1.match_t(24.024)

    q2 = AvroQuery()
    q2.match_server("NHLPlacementDetector")
    q2.match_value("LED Dasherboard")

    Q.add(q1)
    Q.add(q2)

    results = det_querier.group_query(Q, "region_id")
    assert len(results) == 1
