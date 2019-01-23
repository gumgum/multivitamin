import os
import sys
import argparse
import yaml
import json
import glog as log
from collections import defaultdict

from vitamincv.avro_api.avro_api import AvroAPI, AvroIO
from vitamincv.avro_api.cv_schema_factory import *
from vitamincv.media_api.media import MediaRetriever
from vitamincv.avro_api.utils import round_all_pts_in_contour, round_float, p0p1_from_bbox_contour

""" Combine annotations
"""

#source_url will be the url and the key to the avroAPI object
def load_yaml(yaml_file):
    with open(yaml_file, "r") as stream:
        try:
            return yaml.load(stream)
        except:
            raise yaml.YAMLError

def read_hams_dir(hdir):
    hams = []
    for root, dirs, files in os.walk(hdir):
        for fi in files:
            hams.append(os.path.join(root, fi))
    return hams

def dict_to_list(frame_anns):
    out = []
    for tstamp, image_ann in frame_anns.items():
        out.append(image_ann[0])
    return out

def get_source_url_from_ham(ham_dict):
    image_anns = ham_dict["media"]
    for ann in image_anns:
        return ann["source_url"]

def split_up_hams(hams_dir):
    hams_per_video = defaultdict(list) #key: url, val = list of files
    hams = read_hams_dir(hams_dir)
    for ham in hams:
        with open(ham, 'r') as rf:
            js = json.load(rf)
            url = get_source_url_from_ham(js)
            hams_per_video[url].append(ham)
    return hams_per_video

def compile_frame_anns(url, ham_files):
    frame_anns = defaultdict(list) #tstamp is key, list of regions is val
    all_tstamps = []
    for ham_file in ham_files:
        log.info("processing: {} for url: {}".format(ham_file, url))
        js = json.load(open(ham_file))
        image_anns = js["media"]
        for image_ann in image_anns:
            regs = []
            tstamp = round_float(float(image_ann["t"]))
            for region in image_ann["region"]:
                if len(region["values"]) == 0:
                    continue
                _reg = create_region(
                        contour = round_all_pts_in_contour(list(reversed(region["contour"]))),
                        props = [create_prop(
                                    confidence=1.0,
                                    property_type=region["values"][0]["category"].lower(),
                                    value=region["values"][0]["value"],
                                    server="HAM",
                                    company="theorem"
                        )])
                (x0, y0), (x1, y1) = p0p1_from_bbox_contour(_reg["contour"], dtype=float)
                region_id = "{}_({},{})({},{})({},{})({},{})".format(tstamp, x0, y0, x1, y0, x1, y1, x0, y1)
                _reg["id"] = region_id
                regs.append(_reg)
            all_tstamps.append(tstamp)
            if regs == []:
                continue
            for reg in regs:
                frame_anns[tstamp].append(reg)
    return frame_anns, list(set(all_tstamps))

def write_avro_doc(avro_doc, outfn, cv_schema_file):
    if AvroIO.validate_schema(avro_doc, open(cv_schema_file).read()):
        log.info("writing to {}".format(outfn))
        AvroIO.write_json(avro_doc, outfn, indent=2)
    else:
        log.info("not valid schema")

def construct_avro_doc(url, frame_anns, eligible_props, tstamps):
    avro_api = AvroAPI()
    avro_api.set_url(url)
    avro_api.set_url_original(url)
    log.info("Retrieving w, h")
    med_ret = MediaRetriever(url)
    w, h = med_ret.get_w_h()
    avro_api.set_dims(w, h)
    avro_api.append_annotation_task(create_annotation_task(labels=eligible_props))
    
    for tstamp, regions in frame_anns.items():
        avro_api.append_image_ann(create_image_ann(t=tstamp, regions=regions))
    avro_api.sort_image_anns_by_timestamp()
    avro_api.set_codes([create_footprint(annotator="theorem", server="HAM", tstamps=tstamps)])
    return avro_api.get_response()

if __name__=="__main__":
    a = argparse.ArgumentParser("python3 unhamify.py --hamify_yaml nhl_placements_task.yml --hams_dir anno_hams --out_dir avros")
    a.add_argument("--hamify_yaml", required=True)
    a.add_argument("--hams_dir", required=True)
    a.add_argument("--cv_schema_file", required=True)
    a.add_argument("--out_dir", default="avros")
    args = a.parse_args()
    config = load_yaml(args.hamify_yaml)
    labels = []
    for label in config["labels"]:
        ltmp = label.split("#")
        for al in ltmp:
            labels.append(al)

    eligible_props = []
    for label in labels:
        eligible_props.append(create_eligible_prop(property_type=config["property_type"].lower(), value=label))
    
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    hams_per_video = split_up_hams(args.hams_dir)
    for url, ham_files in hams_per_video.items():
        frame_anns, tstamps = compile_frame_anns(url, ham_files)
        tstamps.sort()
        outfn = os.path.basename(url) + ".json"
        avro_doc = construct_avro_doc(url, frame_anns, eligible_props, tstamps)
        write_avro_doc(avro_doc, os.path.join(args.out_dir, outfn), args.cv_schema_file)
 