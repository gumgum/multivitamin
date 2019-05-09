"""Single shot multi-box detector in Caffe

Currently, does not support modules in sequence"""
import os
import sys
import glog as log
import numpy as np
import traceback
import inspect
import importlib.util

glog_level = os.environ.get("GLOG_minloglevel", None)

if glog_level is None:
    os.environ["GLOG_minloglevel"] = "1"
    log.info("GLOG_minloglevel isn't set. Setting level to 1 (info)")
    log.info(
        "\nGLOG_minloglevel levels are...\n\
                0 -- Debug\n\
                1 -- Info\n\
                2 -- Warning\n\
                3 -- Error"
    )

SSD_CAFFE_PYTHON = os.environ.get("SSD_CAFFE_PYTHON")
if SSD_CAFFE_PYTHON:
    sys.path.append(os.path.abspath(SSD_CAFFE_PYTHON))

if importlib.util.find_spec("caffe"):
    import caffe
elif SSD_CAFFE_PYTHON:
    raise ImportError(
        f"Cannot find SSD py-caffe in '{SSD_CAFFE_PYTHON}'. Make sure \
          py-caffe is properly compiled there."
    )
else:
    raise ImportError(
        "Install py-caffe, set PYTHONPATH to point to py-caffe, or \
         set enviroment variable SSD_CAFFE_PYTHON."
    )

from google.protobuf import text_format
from caffe.proto import caffe_pb2 as cpb2

from multivitamin.module import ImagesModule
from multivitamin.data.response.utils import (
    crop_image_from_bbox_contour,
    compute_box_area
)
from multivitamin.data.response.dtypes import (
    create_bbox_contour_from_points,
    Region,
    Property,
)
from multivitamin.applications.utils import load_idmap, load_label_prototxt

LAYER_NAME = "detection_out"


class SSDDetector(ImagesModule):
    def __init__(
        self,
        server_name,
        version,
        net_data_dir,
        confidence_min=0.3,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
        gpuid=0,
    ):
        super().__init__(
            server_name,
            version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,
        )
        self.confidence_min = confidence_min
        if not self.prop_type:
            self.prop_type = "object"

        caffe.set_mode_gpu()
        caffe.set_device(gpuid)

        idmap_file = os.path.join(net_data_dir, "labelmap.prototxt")
        self.labelmap = load_label_prototxt(idmap_file)
        log.info(str(len(self.labelmap.keys())) + " labels parsed.")

        self.net = caffe.Net(
            os.path.join(net_data_dir, "deploy.prototxt"),
            os.path.join(net_data_dir, "model.caffemodel"),
            caffe.TEST,
        )
        self.transformer = caffe.io.Transformer(
            {"data": self.net.blobs["data"].data.shape}
        )

        mean_file = os.path.join(net_data_dir, "mean.binaryproto")
        if os.path.exists(mean_file):
            log.info("Setting meanfile")
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file, "rb").read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean("data", meanfile)
        self.transformer.set_transpose("data", (2, 0, 1))

    def process_images(self, images, tstamps, prev_detections=None):
        for frame, tstamp in zip(images, tstamps):
            im = self.transformer.preprocess("data", frame)
            self.net.blobs["data"].data[...] = im
            predictions = self.net.forward()[LAYER_NAME]

            for pred_idx in range(predictions.shape[2]):
                try:
                    confidence = float(predictions[0, 0, pred_idx, 2])
                    if confidence < self.confidence_min:
                        continue
                    index = int(predictions[0, 0, pred_idx, 1])
                    label = self.labelmap[index]
                    xmin = float(predictions[0, 0, pred_idx, 3])
                    ymin = float(predictions[0, 0, pred_idx, 4])
                    xmax = float(predictions[0, 0, pred_idx, 5])
                    ymax = float(predictions[0, 0, pred_idx, 6])

                    contour = create_bbox_contour_from_points(
                        xmin, ymin, xmax, ymax, bound=True
                    )
                    area = compute_box_area(contour)
                    prop = Property(
                            confidence=confidence,
                            confidence_min=self.confidence_min,
                            ver=self.version,
                            server=self.name,
                            value=label,
                            property_type=self.prop_type,
                            fraction=area,
                        )
                    self.response.append_region(
                        t=tstamp, region=Region(contour=contour, props=[prop])
                    )
                except Exception:
                    log.error(traceback.format_exc())
