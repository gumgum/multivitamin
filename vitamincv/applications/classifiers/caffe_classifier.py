import os
import sys
import traceback

import glog as log
import numpy as np
from google.protobuf import text_format

try:
    log.info("GLOG_minloglevel:" + os.environ['GLOG_minloglevel.'] +" 0 - debug, 1 - info, 2 - warnings, 3 - errors")
except:
    log.info("GLOG_minloglevel is not explicitally defined. 0 - debug, 1 - info, 2 - warnings, 3 - errors")   
try:
    import caffe
except:
    try:
        SSD_CAFFE_PYTHON=os.environ['SSD_CAFFE_PYTHON']
        sys.path.append(os.path.abspath(SSD_CAFFE_PYTHON))
        import caffe
    except:
        sys.exit("Install caffe, set PYTHONPATH to point to caffe, or set \
                  enviroment variable SSD_CAFFE_PYTHON.")
from caffe.proto import caffe_pb2

from vitamincv.module.cvmodule import ImageModule
from vitamincv.module.utils import min_conf_filter_predictions
from vitamincv.data.data import create_detection

GPU=True
DEVICE_ID=0
LAYER_NAME = "prob"
N_TOP = 1
CONFIDENCE_MIN=0.1

class CaffeClassifier(ImageModule):
    def __init__(self, server_name, version, net_data_dir, 
                 prop_type=None, prop_id_map=None, module_id_map=None):
        super().__init__(server_name, version, prop_type=prop_type,
                         prop_id_map=prop_id_map, module_id_map=module_id_map)

        if not self.prop_type:
            self.prop_type = "label"
        log.info("Constructing CaffeClassifier")
        if GPU:
            caffe.set_mode_gpu()
            caffe.set_device(DEVICE_ID)

        labels_file=os.path.join(net_data_dir, "labels.txt")
        try:
            with open(labels_file) as f:
                self.labels = f.read().splitlines()
        except:
            log.error("Unable to parse file: " + labels_file)
            log.error(traceback.format_exc())
            exit(1)
            
        self.net = caffe.Net(os.path.join(net_data_dir, 'deploy.prototxt'),
                             os.path.join(net_data_dir, 'model.caffemodel'),
                             caffe.TEST)
        
        mean_file=os.path.join(net_data_dir, 'mean.binaryproto')
                
        self.transformer = caffe.io.Transformer({'data': self.net.blobs['data'].data.shape})
        if os.path.exists(mean_file):
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file , 'rb' ).read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean('data', meanfile)
        self.transformer.set_transpose('data', (2,0,1))

    def preprocess_image(self, image):
        return self.transformer.preprocess('data', image)

    def process_image(self, image, tstamp, prev_det=None):
        self.net.blobs['data'].data[...] = self.preprocess_image(image)
        probs = self.net.forward()[LAYER_NAME][0]
        asc_sorted_prob_idxs = np.argsort(probs)

        label_idx = asc_sorted_prob_idxs[-1]
        confidence = probs[label_idx]
        label = self.labels[label_idx]

        region_id = None
        contour = None
        if prev_det:
            region_id = prev_det.get("region_id")
            contour = prev_det.get("contour")

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
        log.debug("det: " + str(det))
        self.module_data.detections.append(det)
