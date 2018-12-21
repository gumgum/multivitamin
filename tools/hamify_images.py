import os
import sys
import argparse
import json
import random
import yaml

random.seed(1234567890)

def load_yaml(yaml_file):
    with open(yaml_file, "r") as stream:
        try:
            return yaml.load(stream)
        except:
            raise yaml.YAMLError

def get_basename(video_file):
    return os.path.splitext(os.path.basename(video_file))[0]

def create_hams(labels, urls_batches, template_ham, property_type, out_dir):
    for idx, urls in enumerate(urls_batches):
        ham = json.load(open(template_ham, 'r'))
        ham["task_id"] = "_{}".format(idx)
        ham["media"] = []
        for url in urls:
            item = {}
            item["image_url"] = url
            item["t"] = str("0.0")
            item["source_url"] = ""
            item["region"] = []
            ham["media"].append(item)

        for label in labels:
            ham["property_types"][0][property_type]["values"].append(label)

        outfn = "{}/{}.json".format(out_dir, idx)
        if not os.path.exists(os.path.dirname(outfn)):
            os.makedirs(os.path.dirname(outfn))
        with open(outfn, 'w') as wf:
            wf.write(json.dumps(ham, indent=2))

if __name__=="__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--task_yaml")
    a.add_argument("--out_dir", default="hams")
    args = a.parse_args()
    config = load_yaml(args.task_yaml)

    fns = config["images"]
    batch_size = int(config["batch_size"])
    url_batches = [fns[i:i + batch_size] for i in range(0, len(fns), batch_size)]
    create_hams(config["labels"], url_batches, config["template"], config["property_type"], args.out_dir)
