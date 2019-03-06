import os
import sys
import glog as log
import numpy as np
import traceback


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
from google.protobuf import text_format
from caffe.proto import caffe_pb2
from vitamincv.module_api.cvmodule import CVModule
import vitamincv.module_api.server as Server
from vitamincv.avro_api.cv_schema_factory import *


LOGOEXCLUDE=["Garbage", "Messy", "MessyDark"]
GPU=True
DEVICE_ID=0
LAYER_NAME = "prob"
N_TOP = 1
CONFIDENCE_MIN=0.1
class CaffeClassifier(CVModule):
    def __init__(self, server_name, version, net_data_dir,prop_type=None,prop_id_map=None,module_id_map=None):
        super().__init__(server_name, version, prop_type=prop_type,prop_id_map=prop_id_map,module_id_map=module_id_map)
        if not self.prop_type:
            self.prop_type="label"      
        log.info("Constructing CaffeClassifier")
        #log.setLevel("DEBUG")
        if GPU:
            caffe.set_mode_gpu()
            caffe.set_device(DEVICE_ID)

        labels_file=os.path.join(net_data_dir, "labels.txt")
        try:
            with open(labels_file) as f:
                self.labels = f.read().strip().splitlines()
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

    def process_images(self, images, tstamps, prev_detections=None):
        log.debug("Processing images")
        log.debug("tstamps: "  + str(tstamps))
        log.check_eq(len(images), len(tstamps))
        if prev_detections:
            log.check_eq(len(images), len(prev_detections))
        for i,(frame, tstamp) in enumerate(zip(images, tstamps)):
            log.debug("tstamp: " +str(tstamp))
            crop=frame
            contour_prev=[create_point(0.0, 0.0),
                                 create_point(1.0, 0.0),
                                 create_point(1.0, 1.0),
                                 create_point(0.0, 1.0)]
            region_id_prev=""
            if prev_detections:
                prev_det=prev_detections[i]
                #log.debug("prev_det: " + str(prev_det))
                #we get region_id_prev
                region_id_prev=prev_det['region_id']
                log.debug("region_id_prev: " + str(region_id_prev))
                #we get the contour_prev
                contour_prev=prev_det['contour']
                height = frame.shape[0]
                width = frame.shape[1]
                #we get the crop
                xmin=1.0
                xmax=0.0
                ymin=1.0
                ymax=0.0
                for p in contour_prev:
                    x=p['x']
                    y=p['y']
                    if x<xmin:
                        xmin=x
                    if x>xmax:
                        xmax=x
                    if y<ymin:
                        ymin=y
                    if y>ymax:
                        ymax=y
                log.debug('[xmin,ymin,xmax,ymax]: ' + str([xmin,ymin,xmax,ymax]))
                xmin=int(xmin*(width-1))
                ymin=int(ymin*(height-1))
                xmax=int(xmax*(width-1))
                ymax=int(ymax*(height-1))
                log.debug('Cropping image with [xmin,xmax,ymin,ymax]: ' + str([xmin,xmax,ymin,ymax]))                
                crop=frame[ymin:ymax, xmin:xmax]
            try:    
                im = self.transformer.preprocess('data', crop)
                self.net.blobs['data'].data[...] = im
            
                probs = self.net.forward()[LAYER_NAME]
                log.debug('probs: ' + str(probs))           
                log.debug('probs.shape: ' + str(probs.shape))
                target_shape=(1,len(self.labels))
                if (probs.shape==target_shape)==False:
                    log.debug('Changing shape ' + str(probs.shape) + '->'+ str(target_shape))
                    probs=np.reshape(probs,target_shape)
                    
                for p in probs:
                    log.debug('p: ' + str(p))
                    p_indexes = np.argsort(p)
                    p_indexes = np.flip(p_indexes,0)
                    while True:
                        if len(p_indexes)==1:
                            break
                        index=p_indexes[0]
                        label=self.labels[index]                            
                        log.debug("label: " + str(label))
                        if label in LOGOEXCLUDE:
                            p_indexes=np.delete(p_indexes,0)
                        else:
                            break
                    p_indexes = p_indexes[:N_TOP]
                    
                    log.debug("p_indexes: " + str(p_indexes))
                    for i,property_id in enumerate(p_indexes):
                        if i==N_TOP:
                            break
                        index=p_indexes[i]
                        label=self.labels[index]                            
                        confidence=p[index]
                        
                        if confidence<CONFIDENCE_MIN:
                            label='Unknown'
                        det = create_detection(
                            server=self.name,
                            ver=self.version,
                            value=label,
                            region_id=region_id_prev,
                            contour=contour_prev,
                            property_type=self.prop_type,
                            confidence=confidence,
                            t=tstamp
                        )
                        log.debug("det: " + str(det))                        
                        self.detections.append(det)
                        
            except:
                log.error(traceback.format_exc())
