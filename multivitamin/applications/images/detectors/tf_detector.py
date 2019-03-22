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

if importlib.util.find_spec("tensorflow"):
    import tensorflow as tf
else:
    raise ImportError(
        "Install tensorflowgpu 1.9.0"
    )

from google.protobuf import text_format

from multivitamin.utils.GPUUtilities import GPUUtility
from multivitamin.module import ImagesModule
from multivitamin.data.response.utils import (
    crop_image_from_bbox_contour,
    compute_box_area
)
from multivitamin.data.response.dtypes import (
    create_bbox_contour_from_points,
    Region,
    Property,
    Point
)
from multivitamin.applications.utils import load_idmap, load_label_prototxt

CONFIDENCE_MIN = 0.3


class TFDetector(ImagesModule):
    def __init__(
        self, 
        server_name, 
        version, 
        net_data_dir,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
        **gpukwargs
    ):
        super().__init__(
            server_name, 
            version, 
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map
        )
        self.server_name = server_name
        self.version = version

        if not self.prop_type:
            self.prop_type="object" 
        gpu_util = GPUUtility(**gpukwargs)
        available_devices = gpu_util.get_gpus()
        log.info("Found GPU devices: {}".format(available_devices))
        if available_devices:
            os.environ["CUDA_DEVICE_ORDER"]="PCI_BUS_ID"
            os.environ["CUDA_VISIBLE_DEVICES"]= ",".join([str(gpu) for gpu in available_devices])
        self.label_map = dict()
        labelmap_file = os.path.join(net_data_dir, 'idmap.txt')
        try:
            with open(labelmap_file, 'r') as file_h:
                for line in file_h:
                    label_id, label_name = line.strip().split('\t')
                    label_name = label_name.strip()
                    if label_id not in self.label_map.keys():
                        self.label_map[label_id] = label_name
        except Exception as e:
            log.error("Unable to parse file: " + labelmap_file)
            log.error(traceback.format_exc())
            exit(1)

        model = os.path.join(net_data_dir,'frozen_inference_graph.pb')
        detection_graph = tf.Graph()
        with detection_graph.as_default():
            od_graph_def = tf.GraphDef()
            with tf.gfile.GFile(model, 'rb') as fid:
                serialized_graph = fid.read()
                od_graph_def.ParseFromString(serialized_graph)
                tf.import_graph_def(od_graph_def, name='')
        self._detection_graph = detection_graph
        cfg = tf.ConfigProto()
        cfg.gpu_options.allow_growth = True
        self._sess =  tf.Session(graph=detection_graph, config=cfg)
        

    def process_images(self, images, tstamps, detections_of_interest=None):
        image_tensor = self._detection_graph.get_tensor_by_name('image_tensor:0')
        detection_boxes = self._detection_graph.get_tensor_by_name('detection_boxes:0')
        detection_scores = self._detection_graph.get_tensor_by_name('detection_scores:0')
        detection_classes = self._detection_graph.get_tensor_by_name('detection_classes:0')
        num_pred_detections = self._detection_graph.get_tensor_by_name('num_detections:0')

        for frame, tstamp in zip(images, tstamps):
            frame_np = np.array(frame)
            frame_np = frame_np[...,::-1]
            frame_np_expanded = np.expand_dims(frame_np, axis=0)
            (detections, scores, classes, num) = self._sess.run([detection_boxes, detection_scores, detection_classes, num_pred_detections], feed_dict={image_tensor: frame_np_expanded})
            num = int(num[0])
            for det_idx in range(num):
                try:
                    confidence = float(scores[0,det_idx])
                    if confidence < CONFIDENCE_MIN:
                        continue
                    xmin = float(detections[0,det_idx,1])
                    ymin = float(detections[0,det_idx,0])
                    xmax = float(detections[0,det_idx,3])
                    ymax = float(detections[0,det_idx,2])
                    contour = [Point(xmin, ymin), Point(xmax, ymin), Point(xmax, ymax), Point(xmin, ymax)]
                    prop = Property(
                           property_type=self.prop_type,
                           server=self.server_name,
                           ver=self.version,
                           value = str(self.label_map[str(int(classes[0,det_idx]))]),
                           confidence=confidence,
                           confidence_min=CONFIDENCE_MIN
                           )
                    region = Region(contour, [prop])
                    self.response.append_region(t=tstamp, region=region)

                except Exception as e:
                    log.error(traceback.format_exc())
