import pprint
import glog as log
import json

from cvapis.avro_api.cv_schema_factory import *
from cvapis.avro_api.avro_api import AvroAPI
from cvapis.avro_api.avro_io import AvroIO

def merge(a1, a2):
    """Merge two AvroAPI"""
    log.info("merging")
    log.info("We get the footprints")
    c1=a1.doc["media_annotation"]["codes"]
    c2=a2.doc["media_annotation"]["codes"]
    ret=AvroAPI()
    log.info("merging footprints, we are assuming they are different. At some point.")
    codes=c1+c2
    ret.doc=a1.doc
    ret.set_footprints(codes)
    dets2=a2.get_detections_from_frame_anns()
    log.info("Appending detections")
    log.debug("len(dets2): " + str(len(dets2)))
    for d in dets2:
        #log.info(str(d))
        #log.info("d[\"t\"]:" + str(d["t"]))
        ret.append_detection(d)
    return ret

def transform(json_dict, schema_str):
    #log.setLevel("DEBUG")
    log.setLevel("INFO")
    #log.setLevel("ERROR")
    ################################################
    #We update the doc on the basis of the input doc
    log.debug(pprint.pformat(json_dict))
    log.debug("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    log.debug("Transforming footprints")
    codes=[]
    if 'annotationData' in json_dict:
        json_dict=json_dict["annotationData"]
        
    #log.debug("json_dict[\"media_annotation\"][\"url\"]: " + json_dict["media_annotation"]["url"])
    if 'media_annotation' not in json_dict.keys():
        log.warning("Invalid input")
        return ""

    if 'url' not in json_dict["media_annotation"].keys():
        log.warning("Invalid input")
        return ""
    
    if json_dict["media_annotation"]["url"]=="":
        log.warning("No url")
        if json_dict["media_annotation"]["url_original"]=="":
            log.warning("No url_original")
            return "" #################It makes no sense to take this into consideration
        else:
            json_dict["media_annotation"]["url"]=json_dict["media_annotation"]["url_original"]
    codes_in=json_dict["media_annotation"]["codes"]
    if 'array' in codes_in:
        codes_in=codes_in['array']
    for c in codes_in:
        log.debug("Processing code:\n" +  pprint.pformat(c))
        clocal=create_footprint(**c)
        if 'labels' in clocal:
            labels_in=clocal['labels']
            if 'array' in labels_in:
                labels_in=labels_in['array']
            clocal['labels']=labels_in
        if 'tstamps' in clocal:
            tstamps_in=clocal['tstamps']
            if 'array' in tstamps_in:
                tstamps_in=tstamps_in['array']
            clocal['tstamps']=tstamps_in
        log.debug("Generated code:\n" +  pprint.pformat(clocal))
        codes.append(clocal)
    log.debug("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    log.debug("Transforming frames_annotation")
    frames_annotation=[]
    frames_annotation_in=json_dict["media_annotation"]["frames_annotation"]
    if frames_annotation_in!=None:
        if 'array' in frames_annotation_in:
            frames_annotation_in=frames_annotation_in['array']
        for fa in frames_annotation_in:
            regions=[]
            regions_in=fa["regions"]
            if regions_in!=None:
                if 'array' in regions_in:
                    regions_in=regions_in['array']
                for r in regions_in:
                    if 'sub_regions' in r:
                        del r['sub_regions']
                    props=[]
                    for p in r['props']:
                        p_local=create_prop(**p)
                        p_local['relationships']=[]
                        props.append(p_local)
                    r_local=create_region(**r)
                    r_local['props']=props
                    r_local['features']=""
                    regions.append(r_local)
                if 'array' in fa['regions']:
                    fa['regions'].pop('array', None)
                fa_local=create_image_ann(**fa)
                fa_local['regions']=regions
                frames_annotation.append(fa_local)

    log.debug("We create the whole response.")
    media_ann=create_media_ann(**json_dict["media_annotation"])
    if media_ann["tracks_summary"]==None:
        media_ann["tracks_summary"]=[]
    log.debug("media_ann: " + pprint.pformat(media_ann))
    media_ann['media_summary']=[]
    media_ann['codes']=codes
    media_ann['frames_annotation']=frames_annotation


    if json_dict.get("date"):        
        doc=create_response(date=json_dict['date'])
    else:
        doc=create_response()
        if len(codes)>0:
            date =codes[0]['date']
            doc=create_response(date=date)
        else:
            doc=create_response()
        

    log.debug("*************************")
    log.debug("*************************")
    log.debug("*************************")
    log.debug("EMPTY JSON")
    log.debug(pprint.pformat(doc))
    doc['media_annotation']=media_ann
    #doc['media_annotation']['codes']=codes
    #doc['media_annotation']['frames_annotation']=frames_annotation
    log.debug("*************************")
    log.debug("*************************")
    log.debug("*************************")
    log.debug(pprint.pformat(doc))
    #input()
    #################################################
    valid=AvroIO.is_valid_avro_doc_static(doc, schema_str)
    if valid==False:
        log.error("json not valid.")
        exit(1)
        return ""
    else:
        log.debug("json is valid.")

    json_out=json.dumps(doc)
    return json_out
