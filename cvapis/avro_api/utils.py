import os
import sys
import glog as log
import pprint
import json
import datetime
import cv2

from cvapis.avro_api import config
from cvapis.avro_api.cv_schema_factory import *
from cvapis.media_api.media import MediaRetriever

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

def round_all_pts_in_contour(contour):
    """Function to round all pts in a contour

    Args:
        contour (list): list of dicts with keys x, y
    
    Returns:
        list: of dicts of points
    """
    rounded_contour = []
    for pt in contour:
        rounded_contour.append(create_point(x=round_float(pt["x"]), y=round_float(pt["y"])))
    return rounded_contour

def round_all_pts_in_contour_to_str(contour):
    """Function to round all pts in a contour

    Args:
        contour (list): list of dicts with keys x, y
    
    Returns:
        list: of dicts of points
    """
    rounded_contour = []
    for pt in contour:
        pt_local=create_point(x=pt["x"], y=pt["y"])
        pt_local["x"]=round_float_to_str(pt_local["x"])
        pt_local["y"]=round_float_to_str(pt_local["y"])
        rounded_contour.append(pt_local)
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

def times_equal(val0, val1):
    """Function for float quality comparison for time values

    Args:
        val0 (float): first value
        val1 (float): second value
    
    Returns:
        bool: equality
    """
    return abs(round_float(val0) - round_float(val1)) < config.TIME_EPS

def get_current_time():
    """Get string YYYYMMDDHHMMSS for current time

    Returns:
        current_time (str)
    """
    now = datetime.datetime.now()
    date = "{:4}{:2}{:2}{:2}{:2}{:2}".format(
        str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2),
        str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)
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
    
def to_SSD_ann_format(avro_api, property_type, out_dir):
    """Convert frame annotations from a single avro doc to SSD annotation format for training

    SSD ann format: x0 y0 x1 y1 label_integer

    Args:
        avro_api (AvroAPI): an avro document
        property_type (str): identify property_type to create SSD for
        out_dir (str): where to write the SSD ann files

    NOT COMPLETED
    TODO: parallelize with thread workers
    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    med_ret = MediaRetriever(avro_api.get_url())
    w,h = med_ret.get_w_h()

    # identify labels
    labels = []
    for tasks in avro_api.doc["media_annotation"]["annotation_tasks"]:
        for label in tasks["labels"]:
            if label["property_type"] == property_type:
                labels.append(label["value"])
    labels = sorted(set(labels))
    label2idx = {val:i+1 for i, val in enumerate(labels)}
    
    # create idmap.txt
    with open(os.path.join(out_dir, "idmap.txt"), 'w') as wf:
        wf.write("0\tbackground\n")
        for i, label in enumerate(labels):
            wf.write("{}\t{}\n".format(i+1, label))
    
    # create labelmap.txt
    with open(os.path.join(out_dir, "labelmap.prototxt") , "w") as wf:
        wf.write("item {\n\tname: '" + 'background' + "'\n\tlabel: " + str(0) + "\n\tdisplay_name: '" +label+ "'\n}\n")
        for i, label in enumerate(labels):
            wf.write("item {\n\tname: '" + label + "'\n\tlabel: " + str(i+1) + "\n\tdisplay_name: '" +label+ "'\n}\n")

    img_dir = os.path.join(out_dir, 'imgs')
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)
    
    ann_dir = os.path.join(out_dir, 'anns')
    if not os.path.exists(ann_dir):
        os.makedirs(ann_dir)
    
    listfile = open(os.path.join(out_dir, 'listfile.txt'), 'w')
    detections = avro_api.get_detections()
    w, h = med_ret.get_w_h()

    for det in detections:
        if det["property_type"] != property_type:
            print('''det["property_type"] != property_type:''')
            continue

        tstamp = det["t"]
        frame = med_ret.get_frame(tstamp)
        p0, p1 = p0p1_from_bbox_contour(det["contour"], w, h)

        ann_file = open(os.path.join(ann_dir, '{}.txt'.format(tstamp)), 'a')
        ann_file.write("{} {} {} {} {}\n".format(label2idx[det["value"]], p0[0], p0[1], p1[0], p1[1]))
        ann_file.close()

        img_file = os.path.join(img_dir, '{}.jpg'.format(tstamp))
        if os.path.exists(img_file):
            continue
        cv2.imwrite(img_file, frame)
        listfile.write("{}\t{}\n".format(img_file, ann_file.name))

    listfile.close()


def p0p1_from_bbox_contour(contour, w=1, h=1, dtype=int):
    """Convert cv_schema `contour` into p0 and p1 of a bounding box.

    Args:
        contour (list): list dict of points x, y
        w (int): width
        h (int): height

    Returns:
        Two points dict(x, y): p0 (upper left) and p1 (lower right)
    """
    if (len(contour) != 4):
        log.error("To use p0p1_from_bbox_contour(), input must be a 4 point bbox contour")
        return None

    # Convert number of pixel to max pixel index
    w_max_px_ind = max(w-1, 1)
    h_max_px_ind = max(h-1, 1)

    x0 = contour[0]['x']
    y0 = contour[0]['y']
    x1 = contour[0]['x']
    y1 = contour[0]['y']
    for pt in contour:
        x0 = min(x0, pt['x'])
        y0 = min(y0, pt['y'])
        x1 = max(x1, pt['x'])
        y1 = max(y1, pt['y'])

    x0 = dtype(x0 * w_max_px_ind)
    y0 = dtype(y0 * h_max_px_ind)
    x1 = dtype(x1 * w_max_px_ind)
    y1 = dtype(y1 * h_max_px_ind)
    return (x0, y0), (x1, y1)

def create_region_id(tstamp, contour):
    """Create a region_id

    Args:
        tstamp (float): timestamp
        contour (dict[float]): points of contour

    Returns:
        str: region_id
    """
    tstamp = round_float_to_str(tstamp)
    contour = round_all_pts_in_contour_to_str(contour)
    assert(len(contour)==4)
    xmin = contour[0].get("x")
    xmax = contour[1].get("x")
    ymin = contour[0].get("y")
    ymax = contour[2].get("y")
    
    return "{}_({},{})({},{})({},{})({},{})".format(
        tstamp, xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax
    )

# def jaccard_distance_between_contours(contourA, contourB, w, h):
#     pass

def intersection_between_bboxes(bbox0, bbox1):
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
    return max(max(min_x1-max_x0, 0)*max(min_y1-max_y0, 0), 0)


def union_and_intersection_between_bboxes(bbox0, bbox1):
    if type(bbox0) == type([]):
        bbox0 = p0p1_from_bbox_contour(bbox0, dtype=float)
    if type(bbox1) == type([]):
        bbox1 = p0p1_from_bbox_contour(bbox1, dtype=float)

    p00, p01 = bbox0
    p10, p11 = bbox1
    area0 = (p01[0]-p00[0])*(p01[1]-p00[1])
    area1 = (p11[0]-p10[0])*(p11[1]-p10[1])

    intersection = intersection_between_bboxes(bbox0, bbox1)
    union = area0 + area1 - intersection

    return union, intersection

def jaccard_distance_between_bboxes(bbox0, bbox1):
    union, intersection = union_and_intersection_between_bboxes(bbox0, bbox1)
    if union == 0:
        return 0
    return intersection/union
