import glog as log
from collections import defaultdict


def create_footprint(
    code="",
    ver="",
    company="gumgum",
    labels=None,
    server_track="",
    server="",
    date="20000101000000",
    annotator="",
    tstamps=None,
    id="",
):
    if not labels:
        labels = []
    if not tstamps:
        tstamps = []
    return {
        "code": code,
        "ver": ver,
        "company": company,
        "labels": labels,
        "server_track": server_track,
        "server": server,
        "date": date,
        "annotator": annotator,
        "tstamps": tstamps,
        "id": id,
    }


def create_bbox_contour_from_points(xmin, ymin, xmax, ymax, bound=False):
    """Helper function to create bounding box contour from 4 extrema points"""
    return [
        create_point(xmin, ymin, bound=bound),
        create_point(xmax, ymin, bound=bound),
        create_point(xmax, ymax, bound=bound),
        create_point(xmin, ymax, bound=bound),
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

    return {"x": x, "y": y}


def create_video_ann(t1=0.0, t2=0.0, region_ids=None, props=None):
    if not region_ids:
        region_ids = []
    if not props:
        props = []

    return {"t1": t1, "t2": t2, "regions": [], "region_ids": region_ids, "props": props}  # OBSOLETE


def create_image_ann(t=0.0, regions=None):
    if not regions:
        regions = []
    return {"t": t, "regions": regions}


def create_region(contour=None, props=None, father_id="", features="", id=""):
    # TODO: create region_id
    if not props:
        props = []
    if not contour:
        contour = [create_point(0.0, 0.0), create_point(1.0, 0.0), create_point(1.0, 1.0), create_point(0.0, 1.0)]
    return {"contour": contour, "props": props, "features": features, "id": id, "father_id": father_id}


def create_prop(
    relationships=None,
    confidence=0.0,
    confidence_min=0.0,
    ver="",
    company="gumgum",
    server_track="",
    value="",
    server="",
    footprint_id="",
    fraction=0.0,
    module_id=0,
    property_type="",
    value_verbose="",
    property_id=0,
):
    if not relationships:
        relationships = []
    return {
        "relationships": relationships,
        "confidence": confidence,
        "confidence_min": confidence_min,
        "ver": ver,
        "company": company,
        "server_track": server_track,
        "value": value,
        "server": server,
        "footprint_id": footprint_id,
        "fraction": fraction,
        "module_id": module_id,
        "property_type": property_type,
        "value_verbose": value_verbose,
        "property_id": property_id,
    }


def create_media_ann(
    codes=None,
    url_original="",
    url="",
    w=0,
    h=0,
    post_url="",
    frames_annotation=None,
    media_summary=None,
    tracks_summary=None,
    annotation_tasks=None,
):

    if not codes:
        codes = []
    if not frames_annotation:
        frames_annotation = defaultdict(list)
    if not media_summary:
        media_summary = []
    if not tracks_summary:
        tracks_summary = []
    if not annotation_tasks:
        annotation_tasks = []
    return {
        "codes": codes,
        "url_original": url_original,
        "url": url,
        "w": w,
        "h": h,
        "post_url": post_url,
        "frames_annotation": frames_annotation,
        "media_summary": media_summary,
        "tracks_summary": tracks_summary,
        "annotation_tasks": annotation_tasks,
    }


def create_response(
    point_aux=None,
    footprint_aux=None,
    proppair_aux=None,
    annotation_task_aux=None,
    image_annotation_aux=None,
    video_annotation_aux=None,
    property_aux=None,
    relationship_aux=None,
    eligibleprop_aux=None,
    region_aux=None,
    media_annotation=None,
    version="",
    date="20000101000000",
):  # date="2000-01-01'T'00:00:00")
    if not media_annotation:
        media_annotation = create_media_ann()
    res = {
        "point_aux": point_aux,
        "footprint_aux": footprint_aux,
        "proppair_aux": proppair_aux,
        "annotation_task_aux": annotation_task_aux,
        "image_annotation_aux": image_annotation_aux,
        "video_annotation_aux": video_annotation_aux,
        "property_aux": property_aux,
        "relationship_aux": relationship_aux,
        "eligibleprop_aux": eligibleprop_aux,
        "region_aux": region_aux,
        "version": version,
        "date": date,
        "media_annotation": media_annotation,
    }
    return res


def create_annotation_task(id="", tstamps=None, labels=None, tags=None):
    if not tstamps:
        tstamps = []
    if not labels:
        labels = []
    if not tags:
        tags = []
    return {"id": id, "tstamps": tstamps, "labels": labels, "tags": tags}


def create_eligible_prop(server="", property_type="", value="", confidence_min=0.0, father_properties=None):
    if not father_properties:
        father_properties = []
    return {
        "server": server,
        "property_type": property_type,
        "value": value,
        "confidence_min": confidence_min,
        "father_properties": father_properties,
    }


def create_prop_pair(property_type="", value=""):
    return {"property_type": property_type, "value": value}


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
    assert len(contour) == 4
    xmin = contour[0].get("x")
    xmax = contour[1].get("x")
    ymin = contour[0].get("y")
    ymax = contour[2].get("y")

    return "{}_({},{})({},{})({},{})({},{})".format(tstamp, xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax)
