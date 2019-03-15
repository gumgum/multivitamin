import os
import pytest
import json
import csv
import boto3

from vitamincv.avro_api.avro_api import AvroAPI
from vitamincv.avro_api.avro_io import AvroIO
from vitamincv.avro_api.cv_schema_factory import *
import zipfile2

import glog as log

avro_str = "avro.json"
avro_bin = "avro.bin"

image_ann = create_image_ann(
    t=5.0,
    regions=[create_region(props=[create_prop(server="test_dummy", value="exists")])],
)

annotation_task_1 = create_annotation_task(
    id="task_dummy_1",
    tstamps=[0],
    labels=[
        create_eligible_prop(
            server="LeagueClassifier",
            property_type="league",
            value="NBA",
            confidence_min=0.9,
            father_properties=[],
        )
    ],
    tags=["dummy_1"],
)


def test_get_detections_from_frame_anns():
    s3_client = boto3.client("s3")
    log.info("Downloading json.")
    tmp_filepath = (
        "/tmp/NHL_GAME_VIDEO_WPGOTT_M2_HOME_20180402_1520698435976.t.mp4.json"
    )
    s3_client.download_file(
        "cvapis-data",
        "jsons/NHL_GAME_VIDEO_WPGOTT_M2_HOME_20180402_1520698435976.t.mp4.json",
        tmp_filepath,
    )
    log.info("JSON downloaded.")
    x = AvroAPI(AvroIO.read_json(tmp_filepath))
    print(json.dumps(x.get_detections_from_frame_anns(), indent=2))


def test_create_avro_str():
    avro_api = AvroAPI()
    avro_api.append_image_ann(image_ann)
    avro_api.set_url("blah")
    AvroIO.write_json(avro_api.get_response(), avro_str, indent=2)
    assert True


def test_create_avro_bin():
    avro_api = AvroAPI()
    avro_api.append_image_ann(image_ann)
    avro_api.set_url("blah")
    avro_io = AvroIO()
    response = avro_api.get_response()
    avro_io.write(response, avro_bin)
    flag = AvroIO.is_valid_avro_doc_static(response, avro_io.get_schema())
    assert flag


def test_create_avro_bin_w_registry():
    avro_api = AvroAPI()
    avro_api.append_image_ann(image_ann)
    avro_api.set_url("blah")
    avro_io = AvroIO()
    avro_io.write(avro_api.get_response(), avro_bin)
    assert True


"""
Example function to test reading json from S3
Json located at https://s3.amazonaws.com/cvapis-data/jsons/Threatpipeline_response0_C.json in SX/AI account
"""


def test_read_avro_json():
    s3_client = boto3.client("s3")
    log.info("Downloading json.")
    tmp_filepath = "/tmp/Threatpipeline_response0_C.json"
    s3_client.download_file(
        "cvapis-data", "jsons/Threatpipeline_response0_C.json", tmp_filepath
    )
    log.info("JSON downloaded.")
    avro_io = AvroIO()
    avro_str = avro_io.read_json(tmp_filepath)
    # log.info("avro_str: " + str(avro_str))
    assert len(avro_str) > 0
    x = AvroAPI(avro_str)


"""
Example function to get detections from json (parsing)
Json located at https://s3.amazonaws.com/cvapis-data/jsons/Threatpipeline_response0_C.json in SX/AI account
"""


def test_query_properties():
    s3_client = boto3.client("s3")
    log.info("Downloading json.")
    tmp_filepath = "/tmp/Threatpipeline_response0_C.json"
    s3_client.download_file(
        "cvapis-data", "jsons/Threatpipeline_response0_C.json", tmp_filepath
    )
    log.info("JSON downloaded.")
    avro_io = AvroIO()
    avro_str = avro_io.read_json(tmp_filepath)
    # log.info("avro_str: " + str(avro_str))
    assert len(avro_str) > 0
    x = AvroAPI(avro_str)
    p = {}
    p["server"] = "ThreatClassifier"
    props = [p]
    dets = x.get_detections_from_props(props)
    assert len(dets) == 1
    log.info("dets: " + str(dets))


"""
Example function to read folder (zipped) of jsons from S3, and get detections from
them based on a filter property; here, server_name
Zipped folder at https://s3.amazonaws.com/cvapis-data/jsons/threat_eg_jsons.zip in SX/AI account
"""


def test_parse_jsons_from_folder():
    s3_client = boto3.client("s3")
    log.info("Downloading zipped jsons.")
    name = "threat_eg_jsons"
    tmp_filepath = "/tmp/" + name + ".zip"
    s3_client.download_file("cvapis-data", "jsons/" + name + ".zip", tmp_filepath)
    log.info("Zipped jsons downloaded.")
    with open(tmp_filepath, "rb") as f:
        log.info("Unzipping it.")
        z = zipfile2.ZipFile(f)
        for name_local in z.namelist():
            print("    Extracting file", name_local)
            z.extract(name_local, "/tmp/")

    jsons_path = "/tmp/" + name
    log.info("Unzipped to" + jsons_path)
    log.info("Parrsing jsons in " + jsons_path)
    all_jsons = os.listdir(jsons_path)
    dets_per_media = {}
    server_name = "ThreatClassifier"
    for ind, single_json_rel in enumerate(all_jsons):
        single_json = jsons_path + "/" + single_json_rel
        log.info("Parsing: " + single_json)
        avro_io = AvroIO()
        x = AvroAPI(avro_io.read_json(single_json))
        p = {}
        p["server"] = server_name
        props = [p]
        dets = x.get_detections_from_props(props)
        url = x.get_url()
        dets_per_media[url] = dets
        log.info("\n\n\n")
        log.info("url: " + url)
        log.info("dets: " + str(dets))


def test_read_avro_bin():
    assert os.path.exists(avro_bin)
    avro_io = AvroIO()
    avro_str = avro_io.decode_file(avro_bin)
    assert avro_str


def test_append_annotation_task():
    avro_api = AvroAPI()
    avro_api.append_image_ann(image_ann)
    avro_api.set_url("blah")
    avro_api.append_annotation_task(annotation_task_1)
    avro_io = AvroIO()
    response = avro_api.get_response()
    #    print(json.dumps(response))
    #   avro_io.encode_to_file(response, avro_bin)
    flag = AvroIO.is_valid_avro_doc_static(response, avro_io.get_schema())
    assert flag
    AvroIO.write_json(response, annotation_task_1["id"] + ".json", 2)


def test_append_trackssummary_from_csv():
    jsons_folder = "../cvapis/data/jsons/"
    # input_json_filepath=jsons_folder + 'Tampa-Bay-Lightning-vs-Montreal-Canadians_03102018_112_108.json'
    # input_csv_filepath=jsons_folder + 'Tampa-Bay-Lightning-vs-Montreal-Canadians_03102018_112_108_GT.csv'
    input_json_filepath = (
        jsons_folder + "NHL_GAME_VIDEO_ANACHI_M2_HOME_20182016_1518124767584.json"
    )
    input_csv_filepath = (
        jsons_folder + "NHL_GAME_VIDEO_ANACHI_M2_HOME_20182016_1518124767584.csv"
    )
    assert os.path.exists(input_json_filepath)
    assert os.path.exists(input_csv_filepath)
    filename = os.path.basename(input_json_filepath)
    filename = os.path.splitext(filename)[0]
    output_json_filepath = jsons_folder + filename + "_With_Human_Tracks.json"
    # we load the input json
    avro_api = AvroAPI(AvroIO.read_json(input_json_filepath))
    # we parse the csv
    cois = ["Sponsor", "Placement", "Start Timestamp", "Stop Timestamp"]
    isponsor = -1
    iplacement = -1
    it1 = -1
    it2 = -1
    with open(input_csv_filepath) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=",")
        titles = []
        placements = []
        sponsors = []
        t1s = []
        t2s = []

        for i, row in enumerate(readCSV):
            if i == 0:
                titles = row
                log.info("titles: " + str(titles))
                log.info("Getting indexes of interest for columns named:  " + str(cois))
                isponsor = titles.index(cois[0])
                iplacement = titles.index(cois[1])
                it1 = titles.index(cois[2])
                it2 = titles.index(cois[3])
                log.info(
                    "indexes: "
                    + str(isponsor)
                    + ", "
                    + str(iplacement)
                    + ", "
                    + str(it1)
                    + ", "
                    + str(it2)
                )
            else:
                placements.append(row[iplacement])
                sponsors.append(row[isponsor])
                t1aux = row[it1]
                t2aux = row[it2]
                if t1aux.find(":") == 1:
                    c1 = t1aux.split(":")
                    c2 = t2aux.split(":")
                    t1 = int(c1[0]) * 3600 + int(c1[1]) * 60 + int(c1[2])
                    t2 = int(c2[0]) * 3600 + int(c2[1]) * 60 + int(c2[2])
                else:  # we assume its seconds
                    t1 = int(t1aux)
                    t2 = int(t2aux)
                t1s.append(t1)
                t2s.append(t2)

        log.info("placements[0:9]: " + str(placements[0:9]))
        log.info("sponsors[0:9]: " + str(sponsors[0:9]))
        log.info("t1s[0:9]: " + str(t1s[0:9]))
        log.info("t2s[0:9]: " + str(t2s[0:9]))
        log.info("placements[-9:]: " + str(placements[-9:]))
        log.info("sponsors[-9:]: " + str(sponsors[-9:]))
        log.info("t1s[-9:]: " + str(t1s[-9:]))
        log.info("t2s[-9:]: " + str(t2s[-9:]))

    for t1, t2, placement, sponsor in zip(t1s, t2s, placements, sponsors):
        # we create the placecement property
        p1 = create_prop(
            confidence=1,
            ver="1.0",
            server="HAM",
            property_type="placement",
            value=placement,
        )
        # we create the sponsor property
        p2 = create_prop(
            confidence=1,
            ver="1.0",
            server="HAM",
            property_type="sponsor",
            value=sponsor,
        )
        ps = [p1, p2]
        # We create the track
        track = create_video_ann(t1=t1, t2=t2, props=ps)
        avro_api.append_track_to_tracks_summary(track)
    AvroIO.write_json(avro_api.get_response(), output_json_filepath, indent=2)
    assert True
    print("done")


# test_append_trackssummary_from_csv()
# def test_avro_created_w_cpp():
#     pass
#
# def test_valid_json_schema():
#     #assert(os.path.exists(avro_bin))
#     assert(os.path.exists(avro_str))
#     #avro_io = AvroIO()
#     #json_str_from_bin = avro_io.decode_file(avro_bin)
#     json_str = AvroIO.read_json(avro_str)
#     #assert(json_str == json_str_from_bin)
#     assert(AvroIO.validate_schema(json_str, schema_str) is True)
#
# def test_invalid_json_schema():
#     json_str = json.load(open("face_media_ann.json"))
#     assert(AvroIO.validate_schema(json_str, schema_str) is False)


#     media_ann = json.loads(open("test.json", "r").read())
#     xavro = avro_api.AvroAPI()
#     xavro.set_media_ann(media_ann)
#
#     p = {}
#     p["company"] = "gumgum"
#     p["server"] = "FaceDetector"
#     p["ver"] = ""
#     p["property_type"] = "label"
#     p["value"] = "face"
#     regions = xavro.get_regions_from_prop(p)
#     print(regions)
#     print(len(regions))
# #check that i can read a string json
