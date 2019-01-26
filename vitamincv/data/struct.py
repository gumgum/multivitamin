class Detection():
    pass

class Segment():
    pass

class CVData():
    def __init__(self, detections=None, segments=None):
        self.detections = detections
        self.segments = segments