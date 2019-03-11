import random

from dataclasses import dataclass, field
from typing import List
from typeguard import typechecked

from vitamincv.data.response.data_impl import DictLike


# Functions for creating default fields

def create_region_id():
    """Create a region_id

    Returns:
        str: 16 digit random number
    """
    return str(int(random.random() * 1e16))


def make_whole_image_contour():
    """Create a list of Points representing an entire 2D image
    
    Returns:
        List[Point]: upper left, upper right, bottom right, bottom left
    """
    return [
        Point(0.0, 0.0),
        Point(1.0, 0.0),
        Point(1.0, 1.0),
        Point(0.0, 1.0)
    ]


# Data classes

@typechecked
@dataclass
class PropPair(DictLike):
    property_type: str = ""
    value: str = ""


@typechecked
@dataclass
class EligibleProp(DictLike):
    server: str = ""
    property_type: str = ""
    value: str = ""
    confidence_min: float = 0.0
    father_properties: List[PropPair] = field(default_factory=list)


@typechecked
@dataclass
class AnnotationTask(DictLike):
    id: str = ""
    tstamps: List[float] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    tags: List[EligibleProp] = field(default_factory=list)


@typechecked
@dataclass
class Footprint(DictLike):
    code: str = ""
    ver: str = ""
    company: str = "gumgum"
    labels: List[EligibleProp] = field(default_factory=list)
    server_track: str = ""
    server: str = ""
    date: str = "20000101000000"
    annotator: str = ""
    tstamps: List[float] = field(default_factory=list)
    id: str = ""


@typechecked
@dataclass
class Point(DictLike):
    x: float = 0.0
    y: float = 0.0
    bound: bool = False
    ub_x: float = 1.0
    ub_y: float = 1.0
    lb_x: float = 0.0
    lb_y: float = 0.0

    def __post_init__(self):
        """Check if point is within bounds defined"""
        if self.bound:
            self.x = min(max(self.lb_x, self.x), self.ub_x)
            self.y = min(max(self.lb_y, self.y), self.ub_y)


@typechecked
@dataclass
class Property(DictLike):
    relationships: List[str] = field(default_factory=list)
    confidence: float = 0.0
    confidence_min: float = 0.0
    ver: str = ""
    company: str = "gumgum"
    server_track: str = ""
    value: str = ""
    server: str = ""
    footprint_id: str = ""
    fraction: float = 0.0
    module_id: int = 0
    property_type: str = ""
    value_verbose: str = ""
    property_id: int = 0

@typechecked
@dataclass
class Region(DictLike):
    contour: List[Point] = field(default_factory=make_whole_image_contour)
    props: List[Property] = field(default_factory=list)
    father_id: str = ""
    features: str = ""
    id: str = field(default_factory=create_region_id)


@typechecked
@dataclass
class VideoAnn(DictLike):
    t1: float = 0.0
    t2: float = 0.0
    props: List[Property] = field(default_factory=list)
    regions: List[Region] = field(default_factory=list)
    region_ids: List[str] = field(default_factory=list)


@typechecked
@dataclass
class ImageAnn(DictLike):
    t: float = 0.0
    regions: List[Region] = field(default_factory=list)


@typechecked
@dataclass
class MediaAnn(DictLike):
    codes: List[Footprint] = field(default_factory=list)
    url_original: str = ""
    url: str = ""
    w: int = 0
    h: int = 0
    post_url: str = ""
    frames_annotation: List[ImageAnn] = field(default_factory=list)
    media_summary: List[VideoAnn] = field(default_factory=list)
    tracks_summary: List[VideoAnn] = field(default_factory=list)
    annotation_tasks: List[AnnotationTask] = field(default_factory=list)


@typechecked
@dataclass
class ModuleResponseInternal(DictLike):
    point_aux = None
    footprint_aux = None
    proppair_aux = None
    annotation_task_aux = None
    image_annotation_aux = None
    video_annotation_aux = None
    property_aux = None
    relationship_aux = None
    eligibleprop_aux = None
    region_aux = None
    media_annotation = None
    version : str = ""
    date: str = "20000101000000"
    media_annotation : MediaAnn = MediaAnn()
    if not media_annotation:
        media_annotation = create_media_ann()