import random
from collections import MutableMapping

from dataclasses import dataclass, field
from typing import List
from typeguard import typechecked

from multivitamin.data.response.utils import round_float

# Helper functions


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
    return [Point(0.0, 0.0), Point(1.0, 0.0), Point(1.0, 1.0), Point(0.0, 1.0)]


def create_bbox_contour_from_points(
    xmin, ymin, xmax, ymax, bound=False, lb_x=0.0, ub_x=1.0, lb_y=0.0, ub_y=1.0
):
    """Helper function to create bounding box contour from 4 extrema points

    Args:
        xmin (float or int): point
        ymin (float or int): point
        xmax (float or int): point
        ymax (float or int): point
        bound (bool, optional): Defaults to False. Bound between lb and ub
        lb_x (float, optional): Defaults to 0.0.
        ub_x (float, optional): Defaults to 1.0.
        lb_y (float, optional): Defaults to 0.0.
        ub_y (float, optional): Defaults to 1.0.

    Returns:
        list[Point]: list of Points
    """
    if bound:
        xmin = max(lb_x, xmin)
        ymin = max(lb_y, ymin)
        xmax = min(ub_x, xmax)
        ymax = min(ub_y, ymax)
    return [Point(xmin, ymin), Point(xmax, ymin), Point(xmax, ymax), Point(xmin, ymax)]


# Data classes


class DictLike(MutableMapping):
    """Base class used for the below dataclasses, so that each dataclass can 
    act like a dict with [] access
    """

    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return f"{super().__repr__()}, ({self.__dict__})"


@typechecked
@dataclass
class PropPair(DictLike):
    property_type: str = ""
    value: str = ""


@typechecked
@dataclass
class Relationship(DictLike):
    relation: str = ""
    sources: List[str] = field(default_factory=list)


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
    """Footprint of a module 

    Args:
        code (str): return code
        ver (str): version
        company (str): company
        labels (List[EligibleProp]): list of labels
        server_track (str): server_track
        date (str): date in YYYYMMDDhhmmss
        annotator (str): annotator
        tstamps (List[float]): list of timestamps processed by module
        id (str): footprint id
        num_images_processed (int): number of images processed
        request_source (str): source of requests, e.g. SQS queue name
    """

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
    num_images_processed: int = 0
    request_source: str = ""


@typechecked
@dataclass
class Point(DictLike):
    """Point class

    Args:
        x (float): Defaults to 0.0
        y (float): Defaults to 0.0
    """

    x: float = 0.0
    y: float = 0.0


@typechecked
@dataclass
class Property(DictLike):
    """Property class

    Args:
        relationships (List[str]): N/A
        confidence (float): Defaults to 0.0
        confidence_min (float): Defaults to 0.0
        ver (str): version
        company (str): Defaults to "gumgum"
        server_track (str): server track
        value (str): value
        server (str): server/module name
        footprint_id (str): associated footprint
        fraction (float): Defaults to 0.0
        module_id (int): Defaults to 0, used for mapping server name to an ID
        property_type (str): type of the value field
        value_verbose (str): extra field for value
        property_id (int): mapping property value to an ID
    """

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

    def __post_init__(self):
        self.t1 = round_float(self.t1)
        self.t2 = round_float(self.t2)


@typechecked
@dataclass
class ImageAnn(DictLike):
    t: float = 0.0
    regions: List[Region] = field(default_factory=list)

    def __post_init__(self):
        self.t = round_float(self.t)


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
class ResponseInternal(DictLike):
    point_aux: Point = None
    footprint_aux: Footprint = None
    proppair_aux: PropPair = None
    annotation_task_aux: AnnotationTask = None
    image_annotation_aux: ImageAnn = None
    video_annotation_aux: VideoAnn = None
    property_aux: Property = None
    relationship_aux: Relationship = None
    eligibleprop_aux: EligibleProp = None
    region_aux: Region = None
    version: str = ""
    date: str = "20000101000000"
    media_annotation: MediaAnn = field(default_factory=MediaAnn)
