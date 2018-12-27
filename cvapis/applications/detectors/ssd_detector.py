import os
import sys
import glog as log
import numpy as np
import traceback

import importlib.util

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

LAYER_NAME = "detection_out"
CONFIDENCE_MIN=0.3

class SSDDetector(CVModule):
    def __init__(self, 
                server_name, 
                version, 
                net_data_dir,
                prop_type = None,
                prop_id_map = None,
                module_id_map = None):
        super().__init__(server_name, 
                            version,
                            prop_type = prop_type,
                            prop_id_map = prop_id_map,
                            module_id_map = module_id_map)
        
        if not self.prop_type:
            self.prop_type="object"      
            
        gpu_util = GPUUtility(**kwargs)
        available_devices = gpu_util.get_gpus()
        if available_devices:
            caffe.set_mode_gpu()
            caffe.set_device(available_devices[0]) # py-caffe only supports 1 GPU

        labelmap_file=os.path.join(net_data_dir, "labelmap.prototxt")
        try:
            with open(labelmap_file) as f:
                labelmap_aux = cpb2.LabelMap()
                text_format.Merge(str(f.read()), labelmap_aux)
                num_labels=len(labelmap_aux.item)
                self.labelmap={}
                for item in labelmap_aux.item:
                    index=item.label
                    label=item.display_name
                    self.labelmap[index]=label                    
                log.info(str(len(self.labelmap.keys()))+ " labels parsed.")                
        except:
            log.error("Unable to parse file: " + labelmap_file)
            log.error(traceback.format_exc())
            exit(1)
            
        self.net = caffe.Net(os.path.join(net_data_dir, 'deploy.prototxt'),
                             os.path.join(net_data_dir, 'model.caffemodel'),
                             caffe.TEST)
        self.transformer = caffe.io.Transformer({'data': self.net.blobs['data'].data.shape})
        
        mean_file=os.path.join(net_data_dir, 'mean.binaryproto')
        if os.path.exists(mean_file):
            blob_meanfile = caffe.proto.caffe_pb2.BlobProto()
            data_meanfile = open(mean_file , 'rb' ).read()
            blob_meanfile.ParseFromString(data_meanfile)
            meanfile = np.squeeze(np.array(caffe.io.blobproto_to_array(blob_meanfile)))
            self.transformer.set_mean('data', meanfile)
        
        self.transformer.set_transpose('data', (2,0,1))

    def process_images(self, images, tstamps, prev_detections=None):
        for frame, tstamp in zip(images, tstamps):
            im = self.transformer.preprocess('data', frame)
            self.net.blobs['data'].data[...] = im

            detections = self.net.forward()[LAYER_NAME]

            _detections = []
            for det_idx in range(detections.shape[2]):                
                try:
                    confidence= detections[0,0,det_idx,2]
                    if confidence<CONFIDENCE_MIN:
                        continue
                    index=int(detections[0,0,det_idx,1])
                    label=self.labelmap[index]
                    xmin = float(detections[0,0,det_idx,3])
                    ymin = float(detections[0,0,det_idx,4])
                    xmax = float(detections[0,0,det_idx,5])
                    ymax = float(detections[0,0,det_idx,6])

                    det = create_detection(
                        contour=[create_point(xmin, ymin),
                                 create_point(xmax, ymin),
                                 create_point(xmax, ymax),
                                 create_point(xmin, ymax)],
                        property_type=self.prop_type,
                        value=label,
                        confidence=detections[0,0,det_idx, 2],
                        t=tstamp
                    )
                    self.detections.append(det)
                    _detections.append(det)
                except:
                    log.error(traceback.format_exc())
            log.debug("frame tstamp: {}".format(tstamp))            
