import glog as log

def create_bbox_contour_from_points(xmin, ymin, xmax, ymax):
    """Helper function to create bounding box contour from 4 extrema points"""
    return [create_2dpoint(xmin, ymin),
            create_2dpoint(xmax, ymin),
            create_2dpoint(xmax, ymax),
            create_2dpoint(xmin, ymax)
            ]

def create_2dpoint(x=0.0, y=0.0, bound=False, ub_x=1.0, ub_y=1.0):
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
    
    class Detection():
    def __init__(self, server="",module_id=0, property_type="label", value="", 
                 value_verbose="", confidence=0.0, fraction=1.0, t=0.0, contour=None,
                 ver="", region_id="", property_id=None, footprint_id="", company="gumgum"):
        """Constructor for Detection data object

            This object is meant to be used as a mediator data structure between our cv schema and moduledata
            
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
            self.dict = {
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

class Detections():
    def __init__():
        self.detections = []
        self.tstamp_map = {}
        
class Segment():
    pass

class ModuleData():
    def __init__(self, detections=None, segments=None):
        self.detections = detections
        self.segments = segments
        self.tstamp_map = create_detections_tstamp_map(detections)

    def create_detections_tstamp_map(detections):
        det_t_map={}
        for d in detections:
            t=d['t']
            if t in det_t_map.keys():
                det_t_map[t].append(d)
            else:
                det_t_map[t]=[d]
        return det_t_map