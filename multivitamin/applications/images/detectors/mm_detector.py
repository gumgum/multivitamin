"""MMDetection detector
https://github.com/open-mmlab/mmdetection/
"""
import os
import sys
import glog as log
import numpy as np
import traceback

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

try:
    import mmcv
except ImportError:
    raise ImportError("package: mmcv not found")

try:
    from mmdet.apis import init_detector, inference_detector
except ImportError:
    raise ImportError("module: mmdet.apis not found")
    
from multivitamin.module import ImagesModule
from multivitamin.data.response.utils import (
    crop_image_from_bbox_contour,
    compute_box_area
)
from multivitamin.data.response.dtypes import (
    create_bbox_contour_from_points,
    Region,
    Property,
)
from multivitamin.applications.utils import load_idmap


class MMDetector(ImagesModule):
    def __init__(
        self,
        server_name,
        version,
        config_file,
        model_file,
        confidence_min=0.3,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
        gpuid=0,
    ):
        """Inference module for https://github.com/open-mmlab/mmdetection/
        
        Args:
            server_name (str): module name
            version (str): version
            config_file (str): path to config_file
            model_file (str): path to model_file
            confidence_min (float, optional): Defaults to 0.3.
            prop_type (str, optional): Defaults to None.
            prop_id_map (dict, optional): Defaults to None.
            module_id_map (dict, optional): Defaults to None.
            gpuid (int, optional): Defaults to 0.
        """
        super().__init__(
            server_name,
            version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,
        )
        self.confidence_min = confidence_min
        if not self.prop_type:
            self.prop_type = "object"

        log.info(f"Loading config_file: {config_file} and model_file: {model_file}")
        cfge = mmcv.Config.fromfile(config_file)
        log.debug(f"config: {cfge}")
        self.model = None
        if not (os.path.exists(config_file) and os.path.exists(model_file)):
            raise ValueError("config_file and/or model_file does not exist")
        try:
            self.model = init_detector(config_file, model_file, device="cuda:{}".format(gpuid))
        except:
            raise ValueError("Could not init_detector")

    def process_images(self, images, tstamps, prev_regions=None):
        for frame, tstamp in zip(images, tstamps):
            predictions = inference_detector(self.model, frame)

            for pred_idx, pred in enumerate(predictions):
                try:
                    for xmin, ymin, xmax, ymax, confidence in pred:
                        if confidence < self.confidence_min:
                            continue
                        label = self.model.CLASSES[pred_idx]

                        xmin_n = xmin / frame.shape[1]
                        xmax_n = xmax / frame.shape[1]
                        ymin_n = ymin / frame.shape[0]
                        ymax_n = ymax / frame.shape[0]

                        contour = create_bbox_contour_from_points(
                            xmin_n, ymin_n, xmax_n, ymax_n, bound=True
                        )

                        area = compute_box_area(contour)

                        prop = Property(
                                confidence=float(confidence),
                                confidence_min=self.confidence_min,
                                ver=self.version,
                                server=self.name,
                                value=label,
                                property_type=self.prop_type,
                                fraction=area,
                            )

                        self.response.append_region(
                            t=tstamp, region=Region(contour=contour, props=[prop])
                        )

                except Exception:
                    log.error(traceback.format_exc())
