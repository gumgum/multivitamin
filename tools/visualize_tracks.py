"""
"""
import os
import sys
import argparse

import cv2

from cvapis.avro_api.avro_api import AvroIO, AvroAPI
from cvapis.avro_api.utils import p0p1_from_bbox_contour
from cvapis.avro_api.cv_schema_factory import *
from cvapis.media_api.media import MediaRetriever

def draw_image_ann(frame, image_ann, w, h, label):
    for region in image_ann["regions"]:
        if region["props"][0]["value"] != label:
            continue
        p0, p1 = p0p1_from_bbox_contour(region["contour"], w, h)
        cv2.rectangle(frame, p0, p1, (0, 255, 0), 2)
        text = "{}: {}".format(region["props"][0]["value"], round(float(region["props"][0]["confidence"]), 2))
        cv2.putText(frame, text, p0, cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2, cv2.LINE_AA)
    return frame

def draw_frames_from_track(track, med_ret, avroapi, track_id, out_dir, label, track_padding):
    if not os.path.exists(os.path.join(out_dir, track_id)):
        os.makedirs(os.path.join(out_dir, track_id))

    t1 = max(0, float(track['t1']) - track_padding)
    t2 = float(track['t2']) + track_padding
    frames = []
    tstamps = []
    w, h = med_ret.get_w_h()
    for frame, tstamp in med_ret.get_frames_iterator(25.0, t1, t2):
        image_ann = avroapi.get_image_ann_from_t(tstamp)
        #region = avroapi.get_region_from_region_id()
        if image_ann:
            drawn_frame = draw_image_ann(frame, image_ann, w, h, label)
            fn = os.path.join(out_dir, track_id, "{:.3f}.jpg".format(tstamp))
            cv2.imwrite(fn, drawn_frame)

def create_track_id(url, track):
    prop = track["regions"][0]["props"][0]
    return "{}_{}_{}_{}_{}".format(os.path.basename(url), prop["property_type"], prop["value"], track["t1"], track["t2"])

def track_length(track):
    return float(track["t2"]) - float(track["t1"])

def visualize_tracks(doc, out_dir, labels=None, track_limit=None, min_conf=0.0, start_time=0.0, min_length=1.0, track_padding=0.0):
    avro_api = AvroAPI(doc)
    med_ret = MediaRetriever(avro_api.get_url())
    for label in labels:
        track_cnt = 0
        query_prop = create_prop(value=label)
        #tracks, confs = avro_api.get_tracks_from_prop(query_prop)
        print("querying for label: {}".format(label))
        tracks = avro_api.get_tracks_from_label(label)
        print("num tracks: {}".format(len(tracks)))
        for track in tracks:
            if track_length(track) < min_length:
                continue
            if float(track["t1"]) < start_time:
                continue
            track_id = create_track_id(avro_api.get_url(), track)
            draw_frames_from_track(track, med_ret, avro_api, track_id, out_dir, label, track_padding)
            track_cnt += 1
            if track_cnt >= track_limit:
                break

if __name__=="__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--json", help="path to avro json doc")
    a.add_argument("--labels", nargs='+', help="optional arg, list of labels of interest")
    a.add_argument("--track_limit", help="max # of tracks to visualize PER LABEL")
    a.add_argument("--min_conf", default=0.0, help="min conf of tracks to visualize")
    a.add_argument("--start_time", default=0.0, help="time from which to start viz in seconds")
    a.add_argument("--out_dir", help="where to write output track viz")
    a.add_argument("--track_pad", default=0.0, help="num seconds to pad tracks for viz")
    a.add_argument("--random", action="store_true", help="flag to select random tracks")
    a.add_argument("--min_track_length", default=1.0, help="min track length for viz")
    args = a.parse_args()

    visualize_tracks(AvroIO.read_json(args.json),
                     args.out_dir,
                     args.labels,
                     int(args.track_limit),
                     float(args.min_conf),
                     float(args.start_time),
                     float(args.min_track_length),
                     float(args.track_pad))
