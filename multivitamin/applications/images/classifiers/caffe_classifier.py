import os
import cv2
import sys
import glog as log
import numpy as np
import traceback
import importlib
import numbers
from datetime import datetime

from multivitamin.module import ImagesModule
from multivitamin.data.response.utils import (
    p0p1_from_bbox_contour,
    crop_image_from_bbox_contour,
)
from multivitamin.module.utils import min_conf_filter_predictions
from multivitamin.data.response.dtypes import Region, Property

glog_level = os.environ.get("GLOG_minloglevel", None)

if glog_level is None:
    os.environ["GLOG_minloglevel"] = "1"
    log.info("GLOG_minloglevel isn't set. Setting level to 1 (info)")
    log.info(
        """GLOG_minloglevel levels are...
                    \t0 -- Debug
                    \t1 -- Info
                    \t2 -- Warning
                    \t3 -- Error"""
    )

CAFFE_PYTHON = os.environ.get("CAFFE_PYTHON")
if CAFFE_PYTHON:
    sys.path.append(os.path.abspath(CAFFE_PYTHON))
if importlib.util.find_spec("caffe"):
    import caffe
elif CAFFE_PYTHON:
    raise ImportError(
        "Cannot find SSD py-caffe in '{}'. Make sure py-caffe is properly compiled there.".format(
            CAFFE_PYTHON
        )
    )
else:
    raise ImportError(
        "Install py-caffe, set PYTHONPATH to point to py-caffe, or set enviroment variable CAFFE_PYTHON."
    )

from caffe.proto import caffe_pb2


LOGOEXCLUDE = ["Garbage", "Messy", "MessyDark"]


class CaffeClassifier(ImagesModule):
    def __init__(
        self,
        server_name,
        version,
        net_data_dir,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
        confidence_min=0.1,
        confidence_min_dict=None,
        layer_name="prob",
        top_n=1,
        postprocess_predictions=None,
        postprocess_args=None,
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
        if confidence_min_dict is None:
            confidence_min_dict = {}
        self.layer_name = layer_name
        self.top_n = top_n
        self.postprocess_override = postprocess_predictions
        self.postprocess_args = postprocess_args
        if not isinstance(self.postprocess_args, tuple):
            self.postprocess_args = ()

        if not self.prop_type:
            self.prop_type = "label"

        log.info("Constructing CaffeClassifier")
        caffe.set_mode_gpu()
        caffe.set_device(gpuid)

        labels_file = os.path.join(net_data_dir, "labels.txt")
        try:
            with open(labels_file) as f:
                self.labels = f.read().strip().splitlines()
        except Exception as err:
            log.error("Unable to parse file: " + labels_file)
            log.error(traceback.format_exc())
            raise ValueError(err)

        self.labels = {idx: label for idx, label in enumerate(self.labels)}
        # Set min conf for all labels to 0, but exclude logos in LOGOEXCLDUE
        self.min_conf_filter = {}
        for idx, label in self.labels.items():
            min_conf = self.confidence_min
            if isinstance(confidence_min_dict.get(label), numbers.Number):
                min_conf = confidence_min_dict[label]
            self.min_conf_filter[label] = min_conf

        self.net = caffe.Net(
            os.path.join(net_data_dir, "deploy.prototxt"),
            os.path.join(net_data_dir, "model.caffemodel"),
            caffe.TEST,
        )
        mean_file = os.path.join(net_data_dir, "mean.binaryproto")

        self.mean = None
        if os.path.exists(mean_file):
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file, "rb").read()
            blob_meanfile.ParseFromString(data_meanfile)
            self.mean = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))

    def process_images(self, images, tstamps,prev_regions,responses):
        log.info("Processing images")
        #log.debug("tstamps: "  + str(tstamps))
        assert len(images) == len(tstamps) == len(prev_regions)
        for i, (frame, tstamp, prev_region, response) in enumerate(
            zip(images, tstamps, prev_regions, responses)
        ):
            log.debug("caffe classifier tstamp: " + str(tstamp))
            try:
                if prev_region is not None:
                    frame = crop_image_from_bbox_contour(frame, prev_region.get("contour"))
                log.debug("Pre transformer")        
                cv2_time = datetime.utcnow()
                im = cv2.resize(frame, self.net.blobs["data"].data.shape[2:])
                im = im.transpose(2, 0, 1)
                if self.mean is not None:
                    im = im - self.mean
                total_cv2= datetime.utcnow() - cv2_time
                log.debug("CV2 Time: " + str(total_cv2))
                log.debug("Post transformer")                
                self.net.blobs["data"].data[...] = im

                # TODO : clean this up
                log.debug("Forward pass before: " + response.url)
                probs = self.net.forward()[self.layer_name]
                log.debug("Forward pass after: " + response.url)
                # log.debug("probs: " + str(probs))
                # log.debug("probs.shape: " + str(probs.shape))
                target_shape = (1, len(self.labels))
                if (probs.shape == target_shape) is False:
                    log.debug(
                        "Changing shape " + str(probs.shape) + "->" + str(target_shape)
                    )
                    probs = np.reshape(probs, target_shape)

                props = []
                for p in probs:
                    # log.debug('p: ' + str(p))
                    p_indexes = np.argsort(p)
                    p_indexes = np.flip(p_indexes, 0)
                    while True:
                        if len(p_indexes) == 1:
                            break
                        index = p_indexes[0]
                        label = self.labels[index]
                        log.debug("label: " + str(label))
                        if label in LOGOEXCLUDE:
                            p_indexes = np.delete(p_indexes, 0)
                        else:
                            break
                    p_indexes = p_indexes[:self.top_n]

                    # log.debug("p_indexes: " + str(p_indexes))

                    for i, property_id in enumerate(p_indexes):
                        if i == self.top_n:
                            break
                        index = p_indexes[i]
                        label = self.labels[index]
                        confidence = p[index]

                        # TODO remove this unknown

                        if confidence < self.confidence_min:
                            label = "Unknown"
                        prop = Property(
                            server=self.name,
                            ver=self.version,
                            value=label,
                            property_type=self.prop_type,
                            confidence=float(confidence),
                            confidence_min=float(self.confidence_min),
                        )
                        if prev_region is not None:
                            prev_region.get("props").append(prop)
                        else:
                            props.append(prop)
                if prev_region is None:
                    response.append_region(
                        t=tstamp, region=Region(props=props)
                    )
            except Exception as e:
                log.error(traceback.print_exc())
                log.error(e)
