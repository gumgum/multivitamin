import os
import sys
import argparse
import cv2
from colour import Color
import random
import json
import glog as log

from cvapis.avro_api.avro_api import AvroIO, AvroAPI
from cvapis.avro_api.utils import p0p1_from_bbox_contour
from cvapis.media_api.media import MediaRetriever

COLORS = ['darkorchid', 'darkgreen', 'coral', 'darkseagreen', 
            'forestgreen', 'firebrick', 'olivedrab', 'steelblue', 
            'tomato', 'yellowgreen']

def get_rand_bgr():
    color = Color(COLORS[random.randint(0, len(COLORS)-1)]).rgb[::-1]
    return [x*255 for x in color]

def get_props_from_region(region):
    prop_strs = []
    for prop in region["props"]:
        out = "{}_{:.2f}".format(prop["value"], prop["confidence"])
        prop_strs.append(out)
    return prop_strs

class FrameDrawer():
    def __init__(self, doc_fn, decode=False, dump=False, out="./tmp"):
        """Given an avro document, draw all frame_annotations
        
        Args:
            doc_fn (str): path to avro doc
            decode (bool): if avro doc is binary, decode
            dump (bool): write images to folder instead of visualizing
            out (str): if writing images, output dir
        """
        doc = None
        aio = AvroIO()
        if decode:
            doc = aio.decode_file(doc_fn)
        else:
            doc = AvroIO.read_json(doc_fn)
            
        if not os.path.exists(out):
            os.makedirs(out)
        self.avro_api = AvroAPI(doc)
        self.dump = dump
        self.out = out
        self.med_ret = MediaRetriever(self.avro_api.get_url())
        self.w, self.h = self.med_ret.get_w_h()
        self.process()

    def process(self):
        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thickness = 2

        for image_ann in self.avro_api.get_image_anns():
            tstamp = image_ann["t"]
            img = self.med_ret.get_frame(tstamp)
            print(json.dumps(image_ann, indent=2))
            for region in image_ann["regions"]:
                rand_color = get_rand_bgr()
                p0, p1 = p0p1_from_bbox_contour(region['contour'], self.w, self.h)
                img = cv2.rectangle(img, p0, p1, rand_color, thickness)
                prop_strs = get_props_from_region(region)
                for i, prop in enumerate(prop_strs):
                    img = cv2.putText(img, prop, (p0[0]+3, p1[1]-3+i*25), face, scale, rand_color, thickness)
            if self.dump:
                outfn = "{}/{}.jpg".format(self.out, tstamp)
                cv2.imwrite(outfn, img)
                log.info("Writing to file: {}".format(outfn))
            else:
                cv2.imshow("{}".format(tstamp), img)
                cv2.waitKey(0)

if __name__=="__main__":
    a = argparse.ArgumentParser("python3 visualize_detections --avro test.avro")
    a.add_argument("--avro", type=str, help="path to avro json doc")
    a.add_argument("--decode", action="store_true", help="if avro is binary")
    a.add_argument("--dump_to_folder", action="store_true", help="write images to file")
    a.add_argument("--dump_dir", default="./tmp")
    args = a.parse_args()


    fd = FrameDrawer(args.avro, args.decode, args.dump_to_folder, args.dump_dir)

