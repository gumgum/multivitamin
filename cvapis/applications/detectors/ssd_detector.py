"""Single shot multi-box detector in Caffe

Currently, does not support modules in sequence"""
import os
import sys
import glog as log
import numpy as np
import traceback
import inspect
import importlib.util

from cvapis.module_api.GPUUtilities import GPUUtility
from cvapis.exceptions import CaffeImportError

glog_level = os.environ.get("GLOG_minloglevel", None)

if glog_level is None:
    os.environ["GLOG_minloglevel"] = "1"
    log.info("GLOG_minloglevel isn't set. Setting level to 1 (info)")
    log.info("\nGLOG_minloglevel levels are...\n\
                0 -- Debug\n\
                1 -- Info\n\
                2 -- Warning\n\
                3 -- Error")


SSD_CAFFE_PYTHON = os.environ.get('SSD_CAFFE_PYTHON')
if SSD_CAFFE_PYTHON:
    sys.path.append(os.path.abspath(SSD_CAFFE_PYTHON))

if importlib.util.find_spec("caffe"):
    import caffe
elif SSD_CAFFE_PYTHON:
    raise CaffeImportError("Cannot find SSD py-caffe in '{}'. Make sure py-caffe is properly compiled there.".format(SSD_CAFFE_PYTHON))
else:
    raise CaffeImportError("Install py-caffe, set PYTHONPATH to point to py-caffe, or set enviroment variable SSD_CAFFE_PYTHON.")

from google.protobuf import text_format
from caffe.proto import caffe_pb2 as cpb2
from cvapis.module_api.cvmodule import CVModule
from cvapis.avro_api.cv_schema_factory import *
from cvapis.avro_api.utils import p0p1_from_bbox_contour, crop_image_from_bbox_contour
from cvapis.applications.utils import load_idmap

LAYER_NAME = "detection_out"
CONFIDENCE_MIN = 0.3

class SSDDetector(CVModule):
    def __init__(self, 
                server_name, 
                version, 
                net_data_dir,
                prop_type = None,
                prop_id_map = None,
                module_id_map = None,
                **gpukwargs):
        super().__init__(server_name, 
                            version,
                            prop_type = prop_type,
                            prop_id_map = prop_id_map,
                            module_id_map = module_id_map)

        if not self.prop_type:
            self.prop_type="object"
            
        gpu_util = GPUUtility(**gpukwargs)
        available_devices = gpu_util.get_gpus()
        log.info("Found GPU devices: {}".format(available_devices))
        if available_devices:
            caffe.set_mode_gpu()
            caffe.set_device(int(available_devices[0])) # py-caffe only supports 1 GPU

        idmap_file = os.path.join(net_data_dir, 'idmap.txt')
        self.labelmap = load_idmap(idmap_file)
        log.info(str(len(self.labelmap.keys())) + " labels parsed.")

        self.net = caffe.Net(os.path.join(net_data_dir, 'deploy.prototxt'),
                             os.path.join(net_data_dir, 'model.caffemodel'),
                             caffe.TEST)
        self.transformer = caffe.io.Transformer({'data': self.net.blobs['data'].data.shape})
        
        mean_file = os.path.join(net_data_dir, 'mean.binaryproto')
        if os.path.exists(mean_file):
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file , 'rb' ).read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean('data', meanfile)
        self.transformer.set_transpose('data', (2,0,1))

    def preprocess_images(self, images, previous_detections=None):
        """Preprocess images for forward pass by cropping regions out using previous detections of interest and using caffe transform

        Args:
            images (list): A list of images to be preprocessed
            previous_detections (list): A list of previous detections of interest

        Returns:
            list: A list of transformed images
        """
        contours = None
        if previous_detections:
            contours = [det.get("contour") if det is not None else None for det in previous_detections]
        
        transformed_images = []

        if type(contours) is not list:
            contours = [None for _ in range(len(images))]

        images = [self._crop_image_from_contour(image, contour) for image, contour in zip(images, contours)]
        
        transformed_images = [self.transformer.preprocess('data', image) for image in images]

        return np.array(transformed_images)

    def _crop_image_from_contour(self, image, contour):
        if contour is None:
            return image

        h = image.shape[0]
        w = image.shape[1]
        (x0, y0), (x1, y1) = p0p1_from_bbox_contour(contour, w=w, h=h)
        
        crop = image[y0:y1, x0:x1]
        return crop

    def process_images(self, images):
        """Network forward pass
        
        Args: 
            images (np.array): A numpy array of images
    
        Returns:
            list: List of tuples corresponding to each detection in the format of
                  (frame_index, label, confidence, xmin, ymin, xmax, ymax)
        """
        self.net.blobs['data'].reshape(*images.shape)
        self.net.blobs['data'].data[...] = images
        preds = self.net.forward()[LAYER_NAME].copy()
        return preds

    def postprocess_predictions(self, predictions):
        """Filters out predictions out per class based on confidence

        Args:
            predictions (list): List of tuples corresponding to confidences between 
                    0 and 1, where each index represents a class

        Returns:
            list: A list of tuples (label, confidence, xmin, ymin, xmax, ymax)
        """
        frame_indexes = np.unique(predictions[:, 0])
        filtered_preds = [[]]*frame_indexes.shape[0]
        
        for pred in predictions:
            frame_index = int(pred[0])
            if pred[2] >= CONFIDENCE_MIN:
                filtered_preds[frame_index].append(pred)
        return np.array(filtered_preds)

    def append_detections(self, prediction_batch, tstamps=None, previous_detections=None):
        """Appends results to detections

        Args:
            prediction_batch (list): A list of lists of prediction tuples 
                    (<class index>, <confidence>) for an image

            tstamps (list): A list of timestamps corresponding to the timestamp of an image

            previous_detections (list): A list of previous detections corresponding
                    to the previous detection of interest of an image
        """
        if tstamps is None:
            raise ValueError("tstamps is None")
        if self.has_previous_detections:
            raise NotImplementedError("previous detections not yet implemented for SSD")

        # if previous_detections is None:
            # previous_detections = [None]* len(prediction_batch)

        baseline_det = create_detection(
                        server = self.name,
                        ver = self.version,
                        property_type = self.prop_type
                    )
        # previous_detections = [baseline_det.copy().update({"t": tstamp}) if prev_det is None else prev_det for tstamp, prev_det in zip(tstamps, previous_detections)]

        log.debug("len(prediction_batch): {}".format(len(prediction_batch)))
        log.debug("len(tstamps): {}".format(len(tstamps)))
        log.debug("len(previous_detections): {}".format(len(previous_detections)))
        # assert(len(prediction_batch)==len(tstamps)==len(previous_detections))
        assert(len(prediction_batch)==len(tstamps))
        for image_preds, tstamp in zip(prediction_batch, tstamps):
            for batch_index, pred, confidence, xmin, ymin, xmax, ymax in image_preds:
                if not isinstance(pred, str):
                    label = self.labelmap.get(pred, inspect.signature(create_detection).parameters["value"].default)
                contour = create_bbox_contour_from_points(xmin, ymin, xmax, ymax)
                det = create_detection(
                        server = self.name,
                        ver = self.version,
                        value = label,
                        contour = contour,
                        property_type = self.prop_type,
                        confidence = confidence,
                        t = tstamp
                    )
                self.detections.append(det)
