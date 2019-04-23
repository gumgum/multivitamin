import random
import datetime
import glog as log
import json

from multivitamin.data.response import config

# from multivitamin.data.response.dtypes import Point


def read_json(file_path):
    """Convenience method for reading jsons"""
    return json.load(open(file_path))


def write_json(json_str, file_path, indent=2):
    """Convenience method for writing jsons"""
    with open(file_path, "w") as wf:
        if type(json_str) is dict:
            json.dump(json_str, wf, indent=indent)
        elif type(json_str) is str:
            wf.write(json_str)
        else:
            raise ValueError(
                "json_str input is not a str or dict. Of type: {}".format(type(json_str))
            )


def p0p1_from_bbox_contour(contour, w=1, h=1, dtype=int):
    """Convert cv_schema `contour` into p0 and p1 of a bounding box.

    Args:
        contour (list): list dict of points x, y
        w (int): width
        h (int): height

    Returns:
        Two points dict(x, y): p0 (upper left) and p1 (lower right)
    """
    if len(contour) != 4:
        log.error("To use p0p1_from_bbox_contour(), input must be a 4 point bbox contour")
        return None

    # Convert number of pixel to max pixel index
    w_max_px_ind = max(w - 1, 1)
    h_max_px_ind = max(h - 1, 1)

    x0 = float(contour[0]["x"])
    y0 = float(contour[0]["y"])
    x1 = float(contour[0]["x"])
    y1 = float(contour[0]["y"])
    for pt in contour:
        x0 = min(x0, float(pt["x"]))
        y0 = min(y0, float(pt["y"]))
        x1 = max(x1, float(pt["x"]))
        y1 = max(y1, float(pt["y"]))

    x0 = dtype(x0 * w_max_px_ind)
    y0 = dtype(y0 * h_max_px_ind)
    x1 = dtype(x1 * w_max_px_ind)
    y1 = dtype(y1 * h_max_px_ind)
    return (x0, y0), (x1, y1)


def compute_box_area(contour):
    """Function to compute the spatial fraction based on the contour
    
    Args:
        contour (List[Point]): contour
    
    Returns:
        float: fraction
    """
    assert len(contour) == 4
    p0, p1 = p0p1_from_bbox_contour(contour, dtype=float)
    return float((p1[0] - p0[0]) * (p1[1] - p0[1]))


def crop_image_from_bbox_contour(image, contour):
    """Crop an image given a bounding box contour
    
    Args:  
        image (np.array): image
        contour (dict[float]): points of a bounding box countour
    
    Returns:
        np.array: image
    """
    if contour is None:
        return image

    h = image.shape[0]
    w = image.shape[1]
    (x0, y0), (x1, y1) = p0p1_from_bbox_contour(contour, w=w, h=h)
    return image[y0:y1, x0:x1]


def round_float(val):
    """Function to round a float to our set number of sigificant digits

    Args:
        val (float): input value
    
    Returns:
        float: rounded float value
    """
    return round(val, config.SIGFIG)


def round_float_to_str(val):
    """Function to round a float to our set number of sigificant digits

    Args:
        val (float): input value
    
    Returns:
        float: rounded float value
    """
    return "{:.4f}".format(val)


# def round_all_pts_in_contour(contour):
#     """Function to round all pts in a contour

#     Args:
#         contour (list): list of dicts with keys x, y

#     Returns:
#         list: of dicts of points
#     """
#     rounded_contour = []
#     for pt in contour:
#         rounded_contour.append(Point(x=round_float(pt["x"]), y=round_float(pt["y"])))
#     return rounded_contour


def round_all_pts_in_contour_to_str(contour):
    """Function to round all pts in a contour

    Args:
        contour (list): list of dicts with keys x, y
    
    Returns:
        list: of dicts of points
    """
    rounded_contour = []
    for pt in contour:
        pt["x"] = round_float_to_str(pt["x"])
        pt["y"] = round_float_to_str(pt["y"])
        rounded_contour.append(pt)
    return rounded_contour


def points_equal(val0, val1):
    """Function for float quality comparison for point values

    Args:
        val0 (float): first value
        val1 (float): second value
    
    Returns:
        bool: equality
    """
    return abs(round_float(val0) - round_float(val1)) < config.POINT_EPS


def times_equal(val0, val1, eps=config.TIME_EPS):
    """Function for float quality comparison for time values

    Args:
        val0 (float): first value
        val1 (float): second value
    
    Returns:
        bool: equality
    """
    if eps is None:
        eps = config.TIME_EPS
    return abs(round_float(val0) - round_float(val1)) < eps


def get_current_time():
    """Get string YYYYMMDDHHMMSS for current time

    Returns:
        current_time (str)
    """
    now = datetime.datetime.now()
    date = "{:4}{:2}{:2}{:2}{:2}{:2}".format(
        str(now.year).zfill(4),
        str(now.month).zfill(2),
        str(now.day).zfill(2),
        str(now.hour).zfill(2),
        str(now.minute).zfill(2),
        str(now.second).zfill(2),
    )
    return date


def get_current_date():
    """Get string YYYYMMDD for current date

    Returns:
        current_date (str)
    """
    now = datetime.datetime.now()
    date = "{:4}{:2}{:2}".format(
        str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2)
    )
    return date


# def jaccard_distance_between_contours(contourA, contourB, w, h):
#     pass


def intersection_between_bboxes(bbox0, bbox1):
    assert len(bbox0) == len(bbox1) == 4
    if type(bbox0) == type([]):
        bbox0 = p0p1_from_bbox_contour(bbox0, dtype=float)
    if type(bbox1) == type([]):
        bbox1 = p0p1_from_bbox_contour(bbox1, dtype=float)
    p00, p01 = bbox0
    p10, p11 = bbox1

    max_x0 = max(p00[0], p10[0])
    max_y0 = max(p00[1], p10[1])
    min_x1 = min(p01[0], p11[0])
    min_y1 = min(p01[1], p11[1])
    return max(max(min_x1 - max_x0, 0) * max(min_y1 - max_y0, 0), 0)


def union_and_intersection_between_bboxes(bbox0, bbox1):
    assert len(bbox0) == len(bbox1) == 4
    if type(bbox0) == type([]):
        bbox0 = p0p1_from_bbox_contour(bbox0, dtype=float)
    if type(bbox1) == type([]):
        bbox1 = p0p1_from_bbox_contour(bbox1, dtype=float)

    p00, p01 = bbox0
    p10, p11 = bbox1
    area0 = (p01[0] - p00[0]) * (p01[1] - p00[1])
    area1 = (p11[0] - p10[0]) * (p11[1] - p10[1])

    intersection = intersection_between_bboxes(bbox0, bbox1)
    union = area0 + area1 - intersection

    return union, intersection


def jaccard_distance_between_bboxes(bbox0, bbox1):
    union, intersection = union_and_intersection_between_bboxes(bbox0, bbox1)
    if union == 0:
        return 0
    return intersection / union
