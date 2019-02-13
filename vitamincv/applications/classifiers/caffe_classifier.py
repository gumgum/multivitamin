import os
import sys
import glog as log
import numpy as np
import traceback
import importlib
import inspect
import numbers

from vitamincv.exceptions import ParseError
from vitamincv.module import ImagesModule
from vitamincv.data.response.utils import p0p1_from_bbox_contour, crop_image_from_bbox_contour
from vitamincv.module.utils import min_conf_filter_predictions
from vitamincv.module.GPUUtilities import GPUUtility
from vitamincv.data.response.data import *

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
        "Cannot find SSD py-caffe in '{}'. Make sure py-caffe is properly compiled there.".format(CAFFE_PYTHON)
    )
else:
    raise ImportError("Install py-caffe, set PYTHONPATH to point to py-caffe, or set enviroment variable CAFFE_PYTHON.")

from caffe.proto import caffe_pb2


# GPU=True
# DEVICE_ID=0
# LAYER_NAME = "prob"
# N_TOP = 1
# CONFIDENCE_MIN=0.1


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
        **gpukwargs,
    ):

        super().__init__(
            server_name, version, prop_type=prop_type, prop_id_map=prop_id_map, module_id_map=module_id_map
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
        gpu_util = GPUUtility(**gpukwargs)
        available_devices = gpu_util.get_gpus()
        if available_devices:
            caffe.set_mode_gpu()
            caffe.set_device(int(available_devices[0]))  # py-caffe only supports 1 GPU

        labels_file = os.path.join(net_data_dir, "labels.txt")
        try:
            with open(labels_file) as f:
                self.labels = f.read().splitlines()
        except Exception as err:
            log.error("Unable to parse file: " + labels_file)
            log.error(traceback.format_exc())
            raise ParseError(err)

        self.labels = {idx: label for idx, label in enumerate(self.labels)}
        # Set min conf for all labels to 0, but exclude logos in LOGOEXCLDUE
        self.min_conf_filter = {}
        for idx, label in self.labels.items():
            min_conf = self.confidence_min
            if isinstance(confidence_min_dict.get(label), numbers.Number):
                min_conf = confidence_min_dict[label]
            self.min_conf_filter[label] = min_conf

        self.net = caffe.Net(
            os.path.join(net_data_dir, "deploy.prototxt"), os.path.join(net_data_dir, "model.caffemodel"), caffe.TEST
        )
        mean_file = os.path.join(net_data_dir, "mean.binaryproto")
        self.transformer = caffe.io.Transformer({"data": self.net.blobs["data"].data.shape})
        if os.path.exists(mean_file):
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file, "rb").read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean("data", meanfile)
        self.transformer.set_transpose("data", (2, 0, 1))

    def process_images(self, image_batch, tstamp_batch, prev_region_batch=None):
        """ Network Forward Pass

        Args:
            images (np.array): A numpy array of images

        Returns:
            list: List of floats corresponding to confidences between 0 and 1,
                    where each index represents a class
        """
        preprocessed_images = self._preprocess_images(image_batch, prev_region_batch)
        preds = self._forward_pass(preprocessed_images)
        postprocessed_preds = self._postprocess_predictions(preds)
        self._append_to_response(postprocessed_preds, tstamp_batch, prev_region_batch)

    def _preprocess_images(self, images, previous_regions=None):
        """Preprocess images for forward pass by cropping contours out using previous regions of interest and using caffe transform

        Args:
            images (list): A list of images to be preprocessed
            previous_regions (list): A list of previous regions of interest

        Returns:
            list: A list of transformed images
        """
        contours = None
        if previous_regions:
            contours = [det.get("contour") if det is not None else None for det in previous_regions]
        if type(contours) is not list:
            contours = [None] * len(images)

        images = [crop_image_from_bbox_contour(image, contour) for image, contour in zip(images, contours)]
        transformed_images = []
        transformed_images = [self.transformer.preprocess("data", image) for image in images]
        return np.array(transformed_images)

    def _forward_pass(self, images):
        self.net.blobs["data"].reshape(*images.shape)
        self.net.blobs["data"].data[...] = images
        preds = self.net.forward()[self.layer_name].copy()
        return preds

    def _postprocess_predictions(self, predictions_batch):
        """Filters out predictions out per class based on confidence,
            and returns the top-N qualifying labels

        Args:
            predictions (list): List of floats corresponding to confidences between
                    0 and 1, where each index represents a class

        Returns:
            list: A list of tuples (<class index>, <confidence>) for the
                    best, qualifying  N-predictions
        """
        if callable(self.postprocess_override):
            processed_preds = self.postprocess_override(predictions_batch, *self.postprocess_args)
            return processed_preds

        preds = np.array(predictions_batch)
        n_top_preds = []
        for pred in preds:
            pred_idxs_max2min = np.argsort(pred)[::-1]
            pred = pred[pred_idxs_max2min]
            # Filter by ignore dict
            pred_idxs_max2min = min_conf_filter_predictions(self.min_conf_filter, pred_idxs_max2min, pred, self.labels)
            # Get Top N
            n_top_pred = list(zip(pred_idxs_max2min, pred))[: self.top_n]
            n_top_preds.append(n_top_pred)

        return n_top_preds

    def _append_to_response(self, preds_batch, tstamp_batch, prev_region_batch):
        """Converts predictions to detections

        Args:
            predictions (list): A list of predictions tuples
                    (<class index>, <confidence>) for an image

            tstamp (float): A timestamp corresponding to the timestamp of an image

            previous_region (dict): A previous detections corresponding
                    to the `previous detection of interest` of an image
        """
        assert len(preds_batch) == len(tstamp_batch) == len(prev_region_batch)

        for top_n_preds, tstamp, region in zip(preds_batch, tstamp_batch, prev_region_batch):
            regions = []
            for pred, conf in top_n_preds:
                if not isinstance(pred, str):
                    pred = self.labels.get(pred, inspect.signature(create_prop).parameters["value"].default)
                log.info(f"Creating region for tstamp: {tstamp}")
                region = create_region(
                    # TODO chagne to data strucutres to check for float  liek below
                    props=create_prop(
                        server=self.name,
                        ver=self.version,
                        value=pred,
                        property_type=self.prop_type,
                        confidence=float(conf),
                        confidence_min=float(self.confidence_min),
                    )
                )
                regions.append(region)
            self.response.append_image_ann(create_image_ann(t=tstamp, regions=regions))

            #     image_ann = create_image_ann(
            #         t=tstamp,

            # )
            # previous_detection = create_detection(
            #     server=self.name, ver=self.version, property_type=self.prop_type, t=tstamp
            # )

        # for pred, confidence in predictions:
        #     if not type(pred) is str:
        #         label = self.labels.get(pred, inspect.signature(create_detection).parameters["value"].default)
        #     else:
        #         label = pred
        #     region_id = previous_detection["region_id"]
        #     contour = previous_detection["contour"]
        #     det = create_detection(
        #         server=self.name,
        #         ver=self.version,
        #         value=label,
        #         region_id=region_id,
        #         contour=contour,
        #         property_type=self.prop_type,
        #         confidence=confidence,
        #         t=tstamp,
        #     )
        #     return det
