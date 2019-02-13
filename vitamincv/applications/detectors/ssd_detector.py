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

from vitamincv.module.GPUUtilities import GPUUtility
from vitamincv.module import ImagesModule
from vitamincv.data.response.utils import crop_image_from_bbox_contour
from vitamincv.data.response.data import *
from vitamincv.applications.utils import load_idmap, load_label_prototxt

LAYER_NAME = "detection_out"
CONFIDENCE_MIN = 0.3


class SSDDetector(ImagesModule):
    def __init__(
        self, server_name, version, net_data_dir, prop_type=None, prop_id_map=None, module_id_map=None, **gpukwargs
    ):
        super().__init__(
            server_name, version, prop_type=prop_type, prop_id_map=prop_id_map, module_id_map=module_id_map
        )

        if not self.prop_type:
            self.prop_type = "object"

        gpu_util = GPUUtility(**gpukwargs)
        available_devices = gpu_util.get_gpus()
        log.info("Found GPU devices: {}".format(available_devices))
        if available_devices:
            caffe.set_mode_gpu()
            caffe.set_device(int(available_devices[0]))  # py-caffe only supports 1 GPU

        idmap_file = os.path.join(net_data_dir, "labelmap.prototxt")
        self.labelmap = load_label_prototxt(idmap_file)
        log.info(str(len(self.labelmap.keys())) + " labels parsed.")

        self.net = caffe.Net(
            os.path.join(net_data_dir, "deploy.prototxt"), os.path.join(net_data_dir, "model.caffemodel"), caffe.TEST
        )
        self.transformer = caffe.io.Transformer({"data": self.net.blobs["data"].data.shape})

        mean_file = os.path.join(net_data_dir, "mean.binaryproto")
        if os.path.exists(mean_file):
            log.info("Setting meanfile")
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file, "rb").read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean("data", meanfile)
        self.transformer.set_transpose("data", (2, 0, 1))

    def process_images(self, image_batch, tstamp_batch, prev_region_batch=None):
        """Network forward pass

        Args:
            images (np.array): A numpy array of images
        """
        transformed_images = np.array([self.transformer.preprocess("data", image) for image in image_batch])
        self.net.blobs["data"].reshape(*transformed_images.shape)
        self.net.blobs["data"].data[...] = transformed_images
        preds_batch = np.squeeze(self.net.forward()[LAYER_NAME])
        for (batch_index, pred, confidence, xmin, ymin, xmax, ymax), tstamp in zip(preds_batch, tstamp_batch):
            if not isinstance(pred, str):
                label = self.labelmap.get(pred, inspect.signature(create_prop).parameters["value"].default)
            else:
                label = pred
            # if confidence < CONFIDENCE_MIN:
            #     continue
            x = [batch_index, pred, confidence, xmin, ymin, xmax, ymax]
            log.info(f"preds: {x} -- tstamp {tstamp} -- label {label}")
            contour = create_bbox_contour_from_points(float(xmin), float(ymin), float(xmax), float(ymax), bound=True)

        # preds = self._forward_pass(preprocessed_images)
        # postprocessed_preds = self._postprocess_predictions(preds)
        # self._append_to_response(preds, tstamp_batch)

    def _forward_pass(self, images):
        """Forward pass of network on images"""
        self.net.blobs["data"].reshape(*images.shape)
        self.net.blobs["data"].data[...] = images
        preds = self.net.forward()[LAYER_NAME].copy()
        return np.squeeze(preds)

    def _preprocess_images(self, images):
        """Preprocess images for forward pass by cropping regions out using previous detections of interest and using caffe transform

        Args:
            images (list): A list of images to be preprocessed
            previous_detections (list): A list of previous detections of interest

        Returns:
            list: A list of transformed images
        """
        transformed_images = []
        transformed_images = [self.transformer.preprocess("data", image) for image in images]
        return np.array(transformed_images)

    # def _postprocess_predictions(self, predictions):
    #     """Filters out predictions out per class based on confidence

    #     Args:
    #         predictions (list): List of tuples corresponding to confidences between
    #                 0 and 1, where each index represents a class

    #     Returns:
    #         list: A list of nd.arrays given by number of detections against (label, confidence, xmin, ymin, xmax, ymax)
    #     """
    #     frame_indexes, indicies_of_first_occurance = np.unique(predictions[:, 0], return_index=True)
    #     predictions = np.split(predictions, indicies_of_first_occurance[1:])
    #     filtered_preds = [preds[preds[:, 2] > CONFIDENCE_MIN] for preds in predictions]
    #     return filtered_preds

    def _append_to_response(self, predictions_batch, tstamp_batch):
        """Converts predictions to detections
        """

        assert(len(predictions_batch) == len(tstamp_batch))

        for preds, tstamps in zip(predictions_batch, tstamp_batch):
            log.info(f"tstamps: {tstamps}")
            for batch_index, pred, confidence, xmin, ymin, xmax, ymax in preds:
                if not isinstance(pred, str):
                    label = self.labelmap.get(pred, inspect.signature(create_prop).parameters["value"].default)
                else:
                    label = pred
                contour = create_bbox_contour_from_points(float(xmin), float(ymin), float(xmax), float(ymax), bound=True)
                log.info(batch_index, pred, confidence, xmin, ymin, xmax, ymax)
            # det = create_detection(
            #     server=self.name,
            #     ver=self.version,
            #     value=label,
            #     contour=contour,
            #     property_type=self.prop_type,
            #     confidence=confidence,
            #     t=tstamp,
            # )
            # yield det

    # def process_images(self, images, tstamps, prev_detections=None):
    #     for frame, tstamp in zip(images, tstamps):
    #         im = self.transformer.preprocess('data', frame)
    #         self.net.blobs['data'].data[...] = im

    #         detections = self.net.forward()[LAYER_NAME]

    #         _detections = []
    #         for det_idx in range(detections.shape[2]):
    #             try:
    #                 confidence= detections[0,0,det_idx,2]
    #                 if confidence<CONFIDENCE_MIN:
    #                     continue
    #                 index=int(detections[0,0,det_idx,1])
    #                 label=self.labelmap[index]
    #                 xmin = float(detections[0,0,det_idx,3])
    #                 ymin = float(detections[0,0,det_idx,4])
    #                 xmax = float(detections[0,0,det_idx,5])
    #                 ymax = float(detections[0,0,det_idx,6])

    #                 det = create_detection(
    #                     contour=[create_point(xmin, ymin),
    #                              create_point(xmax, ymin),
    #                              create_point(xmax, ymax),
    #                              create_point(xmin, ymax)],
    #                     property_type=self.prop_type,
    # 	        		value=label,
    #                     confidence=detections[0,0,det_idx, 2],
    #                     t=tstamp
    #                 )
    #                 self.detections.append(det)
    #                 _detections.append(det)
    #             except:
    #                 log.error(traceback.format_exc())
    #         log.debug("frame tstamp: {}".format(tstamp))
