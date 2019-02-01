import os
import sys
import argparse
import yaml
import json

from vitamincv.avro_api.avro_api import AvroAPI, AvroIO
from vitamincv.avro_api.cv_schema_factory import *
from vitamincv.media_api.media import MediaRetriever

""" Combine annotations
"""

# source_url will be the url and the key to the avroAPI object
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


if __name__ == "__main__":
    a = argparse.ArgumentParser(
        "python3 unhamify.py --hamify_yaml nhl_placements_task.yml --hams_dir anno_hams --out_dir avros"
    )
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
        eligible_props.append(create_eligible_prop(property_type=config["property_type"], value=label))

    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    hams = read_hams_dir(args.hams_dir)

    avros = {}
    for ham in hams:
        js = json.load(open(ham))
        image_anns = js["media"]
        for ham_ann in image_anns:
            url = ham_ann["source_url"]
            if url not in avros:
                # setup avros
                avros[url] = AvroAPI()
                avros[url].set_url(url)
                avros[url].set_url_original(url)
                print(url)
                med_ret = MediaRetriever(url)
                w, h = med_ret.get_w_h()
                avros[url].set_dims(w, h)
                avros[url].append_annotation_task(create_annotation_task(labels=eligible_props))
                avros[url].set_footprints([create_footprint(annotator="theorem", server="HAM")])

            regs = []
            for region in ham_ann["region"]:
                if len(region["values"]) == 0:
                    continue
                _reg = create_region(
                    contour=region["contour"],
                    props=[
                        create_prop(
                            confidence=1.0,
                            property_type=region["values"][0]["category"],
                            value=region["values"][0]["value"],
                            server="HAM",
                            company="theorem",
                        )
                    ],
                )
                regs.append(_reg)
            tstamp = float(ham_ann["t"])
            image_ann = avros[url].get_image_ann_from_t(tstamp)
            if image_ann:
                for reg in regs:
                    avros[url].append_region_to_image_ann(reg, tstamp)
            else:
                avros[url].append_image_ann(create_image_ann(t=tstamp, regions=regs))

    for k, v in avros.items():
        v.sort_image_anns_by_timestamp()
        if AvroIO.is_valid_avro_doc_static(v.get_response(), open(args.cv_schema_file).read()):
            outfn = os.path.basename(k) + ".json"
            print(outfn)
            AvroIO.write_json(v.get_response(), os.path.join(args.out_dir, outfn), indent=2)
