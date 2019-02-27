import os
import sys
import argparse
import cv2
from colour import Color
import random
import json
import glog as log

from vitamincv.data.response import Response, SchemaResponse
from vitamincv.data.utils import p0p1_from_bbox_contour
from vitamincv.media_api.media import MediaRetriever

COLORS = [
    "darkorchid",
    "darkgreen",
    "coral",
    "darkseagreen",
    "forestgreen",
    "firebrick",
    "olivedrab",
    "steelblue",
    "tomato",
    "yellowgreen",
]


def get_rand_bgr():
    color = Color(COLORS[random.randint(0, len(COLORS) - 1)]).rgb[::-1]
    return [x * 255 for x in color]


def get_props_from_region(region):
    prop_strs = []
    for prop in region["props"]:
        out = "{}_{:.2f}".format(prop["value"], prop["confidence"])
        prop_strs.append(out)
    return prop_strs


class FrameDrawer:
    def __init__(self, doc_fn=None, response=None, decode=False, dump=False, out="./tmp"):
        """Given an avro document, draw all frame_annotations
        
        Args:
            doc_fn (str): path to avro doc
            decode (bool): if avro doc is binary, decode
            dump (bool): write images to folder instead of visualizing
            out (str): if writing images, output dir
        """
        try:
            os.makedirs(out)
        except:
            log.warning(out + " already exists.")
        if doc_fn:
            doc = None
            aio = AvroIO()
            if decode:
                doc = aio.decode_file(doc_fn)
            else:
                doc = AvroIO.read_json(doc_fn)

            if not os.path.exists(out):
                os.makedirs(out)
            self.avro_api = AvroAPI(doc)
        else:
            self.avro_api = avro_api

        self.dump = dump
        self.out = out
        self.med_ret = MediaRetriever(self.avro_api.get_url())
        self.w, self.h = self.med_ret.get_w_h()

    def process(self, dump_folder=None, tstamps=None):
        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thickness = 2
        log.info("dump_folder: " + str(dump_folder))
        log.info("tstamps: " + str(tstamps))
        if tstamps == []:  # An empty list of timestamps implies no dumping. None implies dump all frames.
            return
        if not dump_folder:
            dump_folder = self.out
            dump_flag = self.dump
        else:
            dump_flag = True
            if not os.path.exists(dump_folder):
                os.makedirs(dump_folder)

        #we get the image_annotation tstamps
        tstamps_dets=self.avro_api.get_timestamps()
        log.info('tstamps_dets: ' + str(tstamps_dets))
        #we get the frame iterator
        frames_iterator=[]
        try:
            frames_iterator=self.med_ret.get_frames_iterator(sample_rate=1.0)
        except:
            log.error(traceback.format_exc())
            exit(1)

        for i, (img, tstamp) in enumerate(frames_iterator):
            if img is None:
                log.warning("Invalid frame")
                continue
            if tstamp is None:
                log.warning("Invalid tstamp")
                continue
	        #log.info('tstamp: ' + str(tstamp))
            if tstamp not in tstamps_dets:
                continue
            log.info("drawing frame for tstamp: " + str(tstamp))            
            #we get image_ann for that time_stamps
            image_ann=self.avro_api.get_image_ann_from_t(tstamp)
            print(json.dumps(image_ann, indent=2))
            for region in image_ann["regions"]:
                rand_color = get_rand_bgr()
                p0, p1 = p0p1_from_bbox_contour(region["contour"], self.w, self.h)
                img = cv2.rectangle(img, p0, p1, rand_color, thickness)
                prop_strs = get_props_from_region(region)
                for i, prop in enumerate(prop_strs):
                    img = cv2.putText(img, prop, (p0[0] + 3, p1[1] - 3 + i * 25), face, scale, rand_color, thickness)
            if dump_flag:
                outfn = "{}/{}.jpg".format(dump_folder, tstamp)
                cv2.imwrite(outfn, img)
                log.info("Writing to file: {}".format(outfn))
            else:
                cv2.imshow("{}".format(tstamp), img)
                cv2.waitKey(0)
