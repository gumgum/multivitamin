import glog as log
from collections import defaultdict
import json

from vitamincv.data.utils import create_region_id

class MediaData():
    def __init__(self, detections=None, segments=None, meta=None, code=None,
                 prop_id_map=None, module_id_map=None):
        if detections is None:
            detections = []
        self.detections = detections

        if segments is None:
            segments = []
        self.segments = segments

        self.meta = meta
        self.code = code
        self.det_tstamp_map = {}

    def create_detections_tstamp_map(self):
        log.info("Creating detections tstamp map")
        if self.detections is None or self.detections == []:
            log.warning("self.detections is empty; cannot create det_tstamp_map")
            return
        
        if not self.det_tstamp_map:
            self.det_tstamp_map = defaultdict(list)
            for det in self.detections:
                t = det.get("t")
                if t is None:
                    continue
                self.det_tstamp_map[t].append(det)
        log.debug(f"det_tstamp_map: {json.dumps(self.det_tstamp_map, indent=2)}")

    def __repr__(self):
        return f"{self.meta}\nnum_detections: {len(self.detections)}"

def create_metadata(name="", ver="", url="", dims=None, sample_rate=1.0, footprint=None):
    return {
        "name" : name,
        "ver" : ver,
        "url" : url,
        "dims" : dims,
        "sample_rate" : sample_rate,
        "footprint" : footprint
    }

def create_bbox_contour_from_points(xmin, ymin, xmax, ymax, bound=False):
    """Helper function to create bounding box contour from 4 extrema points"""
    return [create_point(xmin, ymin, bound=bound),
            create_point(xmax, ymin, bound=bound),
            create_point(xmax, ymax, bound=bound),
            create_point(xmin, ymax, bound=bound)
        ]

def create_point(x=0.0, y=0.0, bound=False, ub_x=1.0, ub_y=1.0):
    """Create x, y point

    Args:
        x (float): pt x
        y (float): pt y
        bound (bool): if True, enforces [0, ub]
        ub_x (float): upperbound on x if check == True
        ub_y (float): upperbound on y if check == True

    Returns:
        dict: x,y 
    """
    if not isinstance(x, float):
        log.warning("x should be a float")
        x = float(x)
    if not isinstance(y, float):
        log.warning("y should be a float")
        y = float(y)
    lowerbound = 0.0
    if bound:
        x = min(max(lowerbound, x), ub_x)
        y = min(max(lowerbound, y), ub_y)

    return {
        "x": x, 
        "y": y
    }

def create_detection(server="", module_id=0, property_type="label", value="", value_verbose="",
                     confidence=0.0, fraction=1.0, t=0.0, contour=None,
                     ver="", region_id="", property_id=None, footprint_id="", company="gumgum"):
    """Factory method to create a detection object

    This object is meant to be used as a "middle man" representation of frame annotations between our avro cv schema and users.
    Note: type is not enforced.

    Args:
        server (str): name of server
        property_type (str): type of prediction property, e.g. label, placement
        value (str): value of the prediction property, e.g. benchglass
        confidence (float): prediction confidence
        fraction (float): spatial fraction representation region size
        t (float): timestamp of detection
        version (str): server version number
        region_id (str): region identifier
        contour (list[dict]): list of points [0,1]. defaults to box around entire image. 
        property_id (int): property id from idmap

    Returns:
        dict: with the above properties
    """
    if not contour:
        contour = [create_point(0.0, 0.0),
                   create_point(1.0, 0.0),
                   create_point(1.0, 1.0),
                   create_point(0.0, 1.0)]
    
    if not region_id:
        region_id = create_region_id(t, contour)

    return {
        "server" : server,
        "module_id" : module_id,
        "property_type" : property_type,
        "value" : value,
        "value_verbose" : value_verbose,
        "property_id" : property_id,
        "confidence" : confidence,
        "fraction" : fraction,
        "t" : t,
        "company":company,
        "ver" : ver,
        "region_id" : region_id,
        "contour" : contour,
        "footprint_id" : footprint_id
    }

def create_segment(server="", property_type="label", value="", value_verbose="",
                   confidence=0.0, fraction=1.0, t1=0.0, t2=0.0, region_ids=None,
                   version="", property_id=0, track_id=None, company="gumgum"):
    if not region_ids:
        region_ids = []
        
    return {
        "server" : server,
        "property_type" : property_type,
        "value" : value,
        "value_verbose" : value_verbose,
        "property_id" : property_id,
        "confidence" : confidence,
        "fraction" : fraction,
        "t1" : t1,
        "t2": t2,
        "ver" : version,
        "region_ids" : region_ids,
        "track_id": track_id,
        "company": company
    }
