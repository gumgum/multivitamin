import os
import sys
import glog as log
import numpy as np
import traceback
import importlib
import inspect

from cvapis.exceptions import CaffeImportError, ParseError

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
    raise CaffeImportError("Cannot find SSD py-caffe in '{}'. Make sure py-caffe is properly compiled there.".format(CAFFE_PYTHON))
else:
    raise CaffeImportError("Install py-caffe, set PYTHONPATH to point to py-caffe, or set enviroment variable CAFFE_PYTHON.")

from caffe.proto import caffe_pb2
from cvapis.module_api.cvmodule import CVModule
from cvapis.avro_api.cv_schema_factory import * 
from cvapis.avro_api.utils import p0p1_from_bbox_contour
from cvapis.module_api.GPUUtilities import GPUUtility


LOGOEXCLUDE = ["Garbage", "Messy", "MessyDark"]
LAYER_NAME = "prob"
N_TOP = 1
CONFIDENCE_MIN = 0.1

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
                **gpukwargs):

        super().__init__(server_name, 
                            version, 
                            prop_type = prop_type, 
                            prop_id_map = prop_id_map,
                            module_id_map = module_id_map)

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
        self.min_conf_filter = {label:CONFIDENCE_MIN if not label in LOGOEXCLUDE else 1 for idx, label in self.labels.items()} 

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
        (x0, y0), (x1, y1) = p0p1_from_bbox_contour(contour)
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
        preds = self.net.forward()[LAYER_NAME]
        return preds

    def process_properties(self):
        pass

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
        preds = np.array(predictions_batch)
        n_top_preds = []
        for pred in preds:
            pred_idxs_max2min = np.argsort(pred)[::-1]
            # Filter by ignore dict
            pred_idxs_max2min = self.min_conf_filter_predictions(pred_idxs_max2min, pred)
            # Get Top N
            n_top_pred = pred_idxs_max2min[:N_TOP]
            n_top_conf = pred[n_top_pred]
            n_top_preds.append(zip(n_top_pred, n_top_conf))

        return n_top_preds

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
            tstamps = [None for _ in range(len(prediction_batch))]
        tstamps = [inspect.signature(create_detection).parameters["t"].default for tstamp in tstamps if tstamp is None]

        if previous_detections is None:
            previous_detections = [None for _ in range(len(prediction_batch))]

        baseline_det = create_detection(
                        server = self.name,
                        ver = self.version,
                        property_type = self.prop_type
                    )
        previous_detections = [baseline_det.copy().update({"t": tstamp}) if prev_det is None else prev_det for tstamp, prev_det in zip(tstamps, previous_detections)]

        assert(len(prediction_batch)==len(tstamps)==len(previous_detections))
        for image_preds, tstamp, prev_det in zip(prediction_batch, tstamps, previous_detections):
            for pred, confidence in image_preds:
                if not type(pred) is str:
                    label = self.labels.get(pred, inspect.signature(create_detection).parameters["value"].default)
                region_id = prev_det["region_id"]
                contour = prev_det["contour"]
                det = create_detection(
                        server = self.name,
                        ver = self.version,
                        value = label,
                        region_id = region_id,
                        contour = contour,
                        property_type = self.prop_type,
                        confidence = confidence,
                        t = tstamp
                    )
                self.detections.append(det)