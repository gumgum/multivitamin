import os
import sys
import argparse

import glog as log

from multivitamin.data.response.frame_drawer import FrameDrawer


if __name__ == "__main__":
    a = argparse.ArgumentParser("python3 visualize_detections --avro test.avro")
    a.add_argument("--avro", type=str, help="path to avro json doc")
    a.add_argument("--decode", action="store_true", help="if avro is binary")
    a.add_argument("--dump_dir")
    args = a.parse_args()

    dump_to_folder = False
    if args.dump_dir:
        dump_to_folder = True

    fd = FrameDrawer(args.avro, None, args.decode, dump_to_folder, args.dump_dir)
    fd.process()
