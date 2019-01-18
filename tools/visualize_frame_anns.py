import os
import sys
import argparse

import glog as log

from vitamincv.avro_api.frame_drawer import FrameDrawer


if __name__=="__main__":
    a = argparse.ArgumentParser("python3 visualize_detections --avro test.avro")
    a.add_argument("--avro", type=str, help="path to avro json doc")
    a.add_argument("--decode", action="store_true", help="if avro is binary")
    a.add_argument("--dump_to_folder", action="store_true", help="write images to file")
    a.add_argument("--dump_dir", default="./tmp")
    args = a.parse_args()


    fd = FrameDrawer(args.avro, None, args.decode, args.dump_to_folder, args.dump_dir)
    fd.process()
