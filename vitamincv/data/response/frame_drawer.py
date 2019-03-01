import os
from colour import Color
import random
import json
import traceback

import glog as log
import cv2

from vitamincv.data.response import SchemaResponse
from vitamincv.data.io import AvroIO
from vitamincv.data.io.utils import read_json
from vitamincv.data.utils import p0p1_from_bbox_contour
from vitamincv.media import MediaRetriever

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
    def __init__(self, doc_fn=None, schema_response=None, decode=False, dump=False, out="./tmp"):
        """Given an avro document, draw all frame_annotations
        
        Args:
            doc_fn (str): path to avro doc
            decode (bool): if avro doc is binary, decode
            dump (bool): write images to folder instead of visualizing
            out (str): if writing images, output dir
        """
        try:
            os.makedirs(out)
        except Exception as e:
            log.warning(out + " already exists.")
            log.warning(e)
        if doc_fn is not None and schema_response is not None:
            raise ValueError("Both doc_fn and schema_response should not be set")

        dictionary = None
        if doc_fn:
            aio = AvroIO()
            if decode:
                dictionary = aio.decode_file(doc_fn)
            else:
                dictionary = read_json(doc_fn)

            if not os.path.exists(out):
                os.makedirs(out)
            sr = SchemaResponse(dictionary=dictionary)
            self.response = sr.response
        elif schema_response is not None:
            assert isinstance(schema_response, SchemaResponse)
            self.response = schema_response.response

        self.dump = dump
        self.out = out
        self.med_ret = MediaRetriever(self.response.url)
        self.w, self.h = self.med_ret.get_w_h()

    def process(self, dump_folder=None, tstamps=None):
        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thickness = 2
        log.info(f"dump_folder: {dump_folder}")
        if tstamps == [] or tstamps is None:
            log.info("tstamps is empty, doing nothing")
            return
        if not dump_folder:
            dump_folder = self.out
            dump_flag = self.dump
        else:
            dump_flag = True
            if not os.path.exists(dump_folder):
                os.makedirs(dump_folder)

        tstamps = self.schema_response.timestamps
        assert len(tstamps) > 1
        sample_rate = tstamps[1] - tstamps[0]

        log.info(f"tstamps: {tstamps}")

        frames_iterator = []
        try:
            frames_iterator = self.med_ret.get_frames_iterator(sample_rate=sample_rate)
        except Exception as e:
            log.error(e)
            log.error(traceback.format_exc())
            exit(1)

        for i, (img, tstamp) in enumerate(frames_iterator):
            if img is None:
                log.warning("Invalid frame")
                continue
            if tstamp is None:
                log.warning("Invalid tstamp")
                continue
            if tstamp not in tstamps:
                continue

            log.info(f"drawing frame for tstamp: {tstamp}")
            # we get image_ann for that time_stamps
            image_ann = self.response.frame_anns.get(tstamp)
            log.info(json.dumps(image_ann, indent=2))
            for region in image_ann["regions"]:
                rand_color = get_rand_bgr()
                p0, p1 = p0p1_from_bbox_contour(region["contour"], self.w, self.h)
                img = cv2.rectangle(img, p0, p1, rand_color, thickness)
                prop_strs = get_props_from_region(region)
                for i, prop in enumerate(prop_strs):
                    img = cv2.putText(
                        img,
                        prop,
                        (p0[0] + 3, p1[1] - 3 + i * 25),
                        face,
                        scale,
                        rand_color,
                        thickness,
                    )
            if dump_flag:
                outfn = "{}/{}.jpg".format(dump_folder, tstamp)
                cv2.imwrite(outfn, img)
                log.info("Writing to file: {}".format(outfn))
            else:
                cv2.imshow("{}".format(tstamp), img)
                cv2.waitKey(0)
