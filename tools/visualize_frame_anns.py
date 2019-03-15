import os
import sys
import argparse

import glog as log

from multivitamin.data.response.frame_drawer import FrameDrawer


if __name__ == "__main__":
    a = argparse.ArgumentParser("python3 visualize_detections --avro test.avro")
    a.add_argument("--avro", type=str, help="path to avro json doc")
    a.add_argument("--dump_dir", default="./tmp")
    # a.add_argument("--dump_video", action="store_true", help="dumps a video file.")
    # a.add_argument("--dump_images", action="store_true", help="dumps image files.")

    args = a.parse_args()
    avro_api = AvroAPI(doc=args.avro)
    fd = FrameDrawer(avro_api=avro_api, pushing_folder=args.dump_dir)
    fd.process(dump_video=False, dump_images=True)
