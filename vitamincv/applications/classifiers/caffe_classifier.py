import os
import sys
import glog as log
import numpy as np
import traceback
import importlib
import inspect
import numbers

from vitamincv.exceptions import ParseError

glog_level = os.environ.get("GLOG_minloglevel", None)

if glog_level is None:
    os.environ["GLOG_minloglevel"] = "1"
    log.info("GLOG_minloglevel isn't set. Setting level to 1 (info)")
    log.info("""GLOG_minloglevel levels are...
                    \t0 -- Debug
                    \t1 -- Info
                    \t2 -- Warning
                    \t3 -- Error""")


CAFFE_PYTHON = os.environ.get('CAFFE_PYTHON')
if CAFFE_PYTHON:
    sys.path.append(os.path.abspath(CAFFE_PYTHON))

if importlib.util.find_spec("caffe"):
    import caffe
elif CAFFE_PYTHON:
    raise ImportError("Cannot find SSD py-caffe in '{}'. Make sure py-caffe is properly compiled there.".format(CAFFE_PYTHON))
else:
    raise ImportError("Install py-caffe, set PYTHONPATH to point to py-caffe, or set enviroment variable CAFFE_PYTHON.")

from caffe.proto import caffe_pb2
from vitamincv.module_api.cvmodule import CVModule
from vitamincv.avro_api.cv_schema_factory import *
from vitamincv.avro_api.utils import p0p1_from_bbox_contour

from vitamincv.module_api.utils import min_conf_filter_predictions
from vitamincv.module_api.GPUUtilities import GPUUtility


# LOGOEXCLUDE = ["Garbage", "Messy", "MessyDark"]
# LAYER_NAME = "prob"
# N_TOP = 1
# CONFIDENCE_MIN = 0.1

class CaffeClassifier(CVModule):
    """ A generic classifer using the Caffe framework

    Args:
        server_name (str): The name of this classifier to be served
        version (str): This classifier's version
        net_data_dir (str): The path to the caffe net data directory. It should contain
                            files named deploy.prototxt, labels.txt, model.caffemodel, and (optionally)
                            mean.binaryproto
        prop_type (str | optional): The property type returned by the classifier (default: `label`)
        prop_id_map (dict | optional): Converts predicted label to an int
        module_id_map (dict | optional): Converts a server_name to an int
        confidence_min (float | optional): A float describing the minimum confidence
                                                   needed to for a prediction to qualify as
                                                   an output.
        confidence_min_dict (dict | optional): A dict mapping a label string to a
                                               confidence value. All undefined
                                               labels/property_ids will default to
                                               confidence_min
        layer_name (str | optinal): The string name of the desired network layer output
        top_n (int | str): Only use the N most confident, qualifying, predictions
        postprocess_predictions (func | optional): An optional function to allow for custom
                                                   postprocessing. Will overwrite default functionality
        postprocess_args (tuple | optional): Additional args to pass to postprocess_predictions
        gpukwargs: Any added keyword args get passed to the GPUUtility object. Refer to it to see what
                    the keyword args are

    """
    def __init__(self,
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
                **gpukwargs):

        super().__init__(server_name,
                            version,
                            prop_type = prop_type,
                            prop_id_map = prop_id_map,
                            module_id_map = module_id_map)

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
            caffe.set_device(int(available_devices[0])) # py-caffe only supports 1 GPU

        labels_file = os.path.join(net_data_dir, "labels.txt") # Each row in labels.txt is  just a label
        try:
            with open(labels_file) as f:
                self.labels = f.read().splitlines()
        except:
            err = "Unable to parse file: " + labels_file
            log.error(err)
            log.error(traceback.format_exc())
            raise ParseError(err)

        self.labels = {idx:label for idx,label in enumerate(self.labels)}
        # Set min conf for all labels to 0, but exclude logos in LOGOEXCLDUE
        self.min_conf_filter = {}
        for idx, label in self.labels.items():
            min_conf = self.confidence_min
            if isinstance(confidence_min_dict.get(label), numbers.Number):
                min_conf = confidence_min_dict[label]
            self.min_conf_filter[label] = min_conf

        self.net = caffe.Net(os.path.join(net_data_dir, 'deploy.prototxt'),
                             os.path.join(net_data_dir, 'model.caffemodel'),
                             caffe.TEST)

        mean_file = os.path.join(net_data_dir, 'mean.binaryproto')

        self.transformer = caffe.io.Transformer({'data': self.net.blobs['data'].data.shape})
        if os.path.exists(mean_file):
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file , 'rb' ).read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean('data', meanfile)

        self.transformer.set_transpose('data', (2,0,1))

    def preprocess_images(self, images,  previous_detections = None):
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
        """ Network Forward Pass

        Args:
            images (np.array): A numpy array of images

        Returns:
            list: List of floats corresponding to confidences between 0 and 1,
                    where each index represents a class
        """
        self.net.blobs['data'].reshape(*images.shape)
        self.net.blobs['data'].data[...] = images
        preds = self.net.forward()[self.layer_name].copy()
        return preds

    def postprocess_predictions(self, predictions_batch):
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
            # Filter by ignore dict
            pred_idxs_max2min = min_conf_filter_predictions(self.min_conf_filter, pred_idxs_max2min, pred, self.labels)
            # Get Top N
            n_top_pred = pred_idxs_max2min[:self.top_n]
            n_top_conf = pred[n_top_pred]
            n_top_preds.append(list(zip(n_top_pred, n_top_conf)))

        return n_top_preds

    def convert_to_detection(self, predicitons, tstamp=None, previous_detection=None):
        """Converts predictions to detections

        Args:
            predictions (list): A list of predictions tuples
                    (<class index>, <confidence>) for an image

            tstamp (float): A timestamp corresponding to the timestamp of an image

            previous_detection (dict): A previous detections corresponding
                    to the `previous detection of interest` of an image

        """
        if tstamp is None:
            tstamp = inspect.signature(create_detection).parameters["t"].default


        if previous_detection is None:
            previous_detection = create_detection(
                                        server=self.name,
                                        ver=self.version,
                                        property_type=self.prop_type
                                        t=tstamp
                                    )

        for pred, confidence in predicitons:
            if not type(pred) is str:
                label = self.labels.get(pred, inspect.signature(create_detection).parameters["value"].default)
            else:
                label = pred
            region_id = previous_detection["region_id"]
            contour = previous_detection["contour"]
            det = create_detection(
                    server=self.name,
                    ver=self.version,
                    value=label,
                    region_id=region_id,
                    contour=contour,
                    property_type=self.prop_type,
                    confidence=confidence,
                    t=tstamp
                )
            return det
