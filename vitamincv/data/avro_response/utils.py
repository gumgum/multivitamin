import os
import sys
import glog as log
import pprint
import json
import datetime
import tqdm
import cv2

from vitamincv.data.avro_response import config
from vitamincv.data.avro_response.cv_schema_factory import *
from vitamincv.data.utils import p0p1_from_bbox_contour, crop_image_from_bbox_contour
from vitamincv.media import MediaRetriever


def to_SSD_ann_format(avro_api, property_type, out_dir):
    """Convert frame annotations from a single avro doc to SSD annotation format for training

    SSD ann format: x0 y0 x1 y1 label_integer

    Args:
        avro_api (AvroAPI): an avro document
        property_type (str): identify property_type to create SSD for
        out_dir (str): where to write the SSD ann files

    NOT COMPLETED
    TODO: parallelize with thread workers
    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    med_ret = MediaRetriever(avro_api.get_url())
    w,h = med_ret.get_w_h()

    # identify labels
    labels = []
    for tasks in avro_api.doc["media_annotation"]["annotation_tasks"]:
        for label in tasks["labels"]:
            if label["property_type"] == property_type:
                labels.append(label["value"])
    labels = sorted(set(labels))
    label2idx = {val:i+1 for i, val in enumerate(labels)}
    log.info("labels: {}".format(labels))
    log.info("label2idx: {}".format(label2idx))
    
    # create idmap.txt
    with open(os.path.join(out_dir, "idmap.txt"), 'w') as wf:
        wf.write("0\tbackground\n")
        for i, label in enumerate(labels):
            wf.write("{}\t{}\n".format(i+1, label))
    
    # create labelmap.txt
    with open(os.path.join(out_dir, "labelmap.prototxt") , "w") as wf:
        wf.write("item {\n\tname: '" + 'background' + "'\n\tlabel: " + str(0) + "\n\tdisplay_name: '" +label+ "'\n}\n")
        for i, label in enumerate(labels):
            wf.write("item {\n\tname: '" + label + "'\n\tlabel: " + str(i+1) + "\n\tdisplay_name: '" +label+ "'\n}\n")
    
    img_dir = os.path.join(out_dir, 'imgs')
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)
    
    ann_dir = os.path.join(out_dir, 'anns')
    if not os.path.exists(ann_dir):
        os.makedirs(ann_dir)
    
    listfile = open(os.path.join(out_dir, 'listfile.txt'), 'w')
    detections = avro_api.get_detections()
    w, h = med_ret.get_w_h()

    for det in tqdm.tqdm(detections):
        if det["property_type"] != property_type:
            log.error('''det["property_type"]: {} != property_type: {}'''.format(det["property_type"], property_type))
            continue

        tstamp = det["t"]
        frame = med_ret.get_frame(tstamp)
        p0, p1 = p0p1_from_bbox_contour(det["contour"], w, h)

        ann_file = open(os.path.join(ann_dir, '{}.txt'.format(tstamp)), 'a')
        ann_file.write("{} {} {} {} {}\n".format(label2idx[det["value"]], p0[0], p0[1], p1[0], p1[1]))
        ann_file.close()

        img_file = os.path.join(img_dir, '{}.jpg'.format(tstamp))
        if os.path.exists(img_file):
            continue
        cv2.imwrite(img_file, frame)
        listfile.write("{}\t{}\n".format(img_file, ann_file.name))

    listfile.close()
