class Detection():
    pass

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