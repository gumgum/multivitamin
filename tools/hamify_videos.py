import os
import sys
import argparse
import json
import random
import yaml
import requests
import cv2

from cvapis.media_api.media import MediaRetriever

random.seed(1234567890)

s3_bucket_url = "s3://video-ann"
http_url = "https://s3.amazonaws.com/video-ann"

def load_yaml(yaml_file):
    with open(yaml_file, "r") as stream:
        try:
            return yaml.load(stream)
        except:
            raise yaml.YAMLError

def get_basename(video_file):
    return os.path.splitext(os.path.basename(video_file))[0]

def cut_up_image(file, fps, out):
    med_ret = MediaRetriever(file)
    video_name = get_basename(file)
    if not os.path.exists(os.path.join(out, video_name)):
        os.makedirs(os.path.join(out, video_name))

    fns = []
    for frame, tstamp in med_ret.get_frames_iterator(fps):
        tstamp_str = str(tstamp).zfill(14)
        frame_fn = os.path.join(video_name, "{}.jpg".format(tstamp_str))
        cv2.imwrite(os.path.join(out, frame_fn), frame)
        print(frame_fn)
        fns.append(frame_fn)
    print("done cutting up images")
    return video_name, fns

def upload_to_s3(folder, video_name):
    cmd = "aws s3 sync {} {} --acl public-read".format(folder, os.path.join(s3_bucket_url, video_name))
    os.system(cmd)

def batch_fns(fns, batch_size):
    urls = [os.path.join(http_url, fn) for fn in fns]
    tstamps = [float(get_basename(url)) for url in urls]

    url_batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    tstamp_batches = [tstamps[i:i + batch_size] for i in range(0, len(tstamps), batch_size)]
    return url_batches, tstamp_batches

def create_hams(labels, urls_batches, tstamp_batches, template_ham, video_file, video_name, property_type, out_dir):
    for label in labels:
        for idx, (urls, tstamps) in enumerate(zip(urls_batches, tstamp_batches)):
            ham = json.load(open(template_ham, 'r'))
            ham["task_id"] = label + "_{}".format(idx)
            actual_labels = label.split("#") #if no #, doesn't split
            for lb in actual_labels:
                ham["property_types"][0][property_type]["values"].append(lb)

            ham["media"] = []
            for url, tstamp in zip(urls, tstamps):
                item = {}
                item["image_url"] = url
                item["t"] = str(tstamp)
                item["source_url"] = video_file
                item["region"] = []
                ham["media"].append(item)

            outfn = "{}/{}/{}/{}.json".format(out_dir, video_name, ham["task_id"].split("_")[0], ham["task_id"])
            if not os.path.exists(os.path.dirname(outfn)):
                os.makedirs(os.path.dirname(outfn))
            with open(outfn, 'w') as wf:
                wf.write(json.dumps(ham, indent=2))

if __name__=="__main__":
    a = argparse.ArgumentParser("python3 hamify_videos.py --task_yaml nhl_placements_task.yml --out_dir hams")
    a.add_argument("--task_yaml")
    a.add_argument("--out_dir", default="hams")
    args = a.parse_args()
    config = load_yaml(args.task_yaml)

    if not os.path.exists(config["temp_folder"]):
        os.makedirs(config["temp_folder"])

    for video_file in config["video_files"]:
        print("processing {}".format(video_file))
        # video_name, fns = cut_up_image(video_file, config["fps"], config["temp_folder"])
        # upload_to_s3(os.path.join(config["temp_folder"], video_name), video_name)
        
        ### hack
        video_name = get_basename(video_file)
        video_name = video_name.replace(" ", "")
        video_name = video_name.replace("%", "")
        print(video_name)
        fns = [os.path.join(video_name, x) for x in os.listdir(os.path.join(config["temp_folder"], video_name))]
        print(fns)
        ### endhack

        url_batches, tstamp_batches = batch_fns(fns, config["batch_size"])
        create_hams(config["labels"], url_batches, tstamp_batches, config["template"], video_file, video_name, config["property_type"], args.out_dir)
