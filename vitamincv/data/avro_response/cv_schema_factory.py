"""Our current schema does not contain default fields, thus we must initialize defaults here"""
import glog as log


########################################################################
## "private" functions


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
        frames_annotation = []
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
