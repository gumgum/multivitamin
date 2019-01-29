import os
import sys
import glog as log
import pprint
import tempfile
import json
import pprint
import pkg_resources
import numpy as np
import GPUtil
import traceback

from datetime import datetime

from vitamincv.avro_api import config
from vitamincv.avro_api.cv_schema_factory import *
from vitamincv.avro_api.utils import points_equal, times_equal, create_region_id
from vitamincv.avro_api.avro_io import AvroIO
from vitamincv.avro_api.avro_query import *

import importlib.util
if importlib.util.find_spec("cupy"):
    import cupy as cp

class AvroResponse():
    """Wrapper with utilities around a single response document"""
    def __init__(self, doc=None):
        self.set_doc(doc)
    
    def set_doc(self, doc):
        self._clear_maps()
        self.detections = None
        self.doc = create_response()
        self.max_gpu_mem = 0.75
        self.check_for_gpu()
        self.detection_querier = None
        self.segment_querier = None

        if doc:
            if type(doc) is str:
                doc = json.load(open(doc))
            if 'media_annotation' not in doc:
                log.debug("setting media_annotation")
                self.doc["media_annotation"]=doc
            else:
                self.doc = doc

    def reset_queriers(self):
        self.detection_querier = None
        self.segment_querier = None
        
    def check_for_gpu(self):
        self.gpu = None
        deviceIDs = []
        try:
            deviceIDs = GPUtil.getAvailable(order="memory", maxMemory = self.max_gpu_mem, maxLoad = 0.70)
        except:
            log.warning("No GPUs Found -- This must be a CPU only machine")

        if len(deviceIDs) > 0:
            self.gpu = deviceIDs[0]

    """Modifiers"""
    def append_footprint(self,code):
        self.doc["media_annotation"]["codes"].append(code)
        
    def append_detection(self, detection, prop_id_map=None,t_eps=None):
        """Append a detection struct (the middleman between avro schema and API user)
        """        
        log.debug("Appending detection w/ tstamp: {}".format(detection["t"]))

        value=detection.get("value")
        property_id=detection.get("property_id")
        if value is None and property_id is None:
            log.error("detection with no value or property id.")
            return

        if value and property_id and prop_id_map:
            log.warning("Both value and property_id are not None. But there is a property_id map. property_id will be overwritten")          
        if prop_id_map and value:
            property_id=prop_id_map.get(value, 0)
                
        if value is None:
            value=""
        if property_id is None:
            property_id=0        
            
        region_id_query=detection['region_id']
        if len(region_id_query)>0:
            region=self.get_region_from_region_id(region_id_query)
            if region:#If the region already exists
                #log.debug("The region " + region_id_query + "already existed")
                #log.debug ("Creating prop")
                prop=create_prop(
                    server=detection["server"],
                    module_id=detection["module_id"],
                    property_type=detection["property_type"],
                    property_id=property_id,
                    value=value,
                    confidence=float(detection["confidence"]),
                    value_verbose=detection["value_verbose"],
                    fraction=detection["fraction"],
                    footprint_id=detection["footprint_id"],
                    ver=detection["ver"],
                    company=detection["company"]
                )
                #log.debug("Appending prop")
                region['props'].append(prop)
                return
        log.debug("The region " + region_id_query + " did not previously exist.")
        if len(region_id_query)>0:
            region_id=region_id_query
        else:
            region_id=create_region_id(detection["t"], detection["contour"])

        region=self.get_region_from_region_id(region_id)
        if region:#If the region already exists
            log.debug("The region " + region_id_query + "already existed")
            log.debug ("Creating prop")
            prop=create_prop(
                server=detection["server"],
                module_id=detection["module_id"],
                property_type=detection["property_type"],
                property_id=property_id,
                value=value,
                confidence=float(detection["confidence"]),
                value_verbose=detection["value_verbose"],
                fraction=detection["fraction"],
                footprint_id=detection["footprint_id"],
                ver=detection["ver"],
                company=detection["company"]
            )
            log.debug("Appending prop")
            region['props'].append(prop)
            return
        
        log.debug("The region " + region_id_query + " did not previously exist.")
        
        region = create_region(
            props=[create_prop(
            server=detection["server"],
            module_id=detection["module_id"],
            property_type=detection["property_type"],
            property_id=property_id,
            value=value,
            confidence=float(detection["confidence"]),
            value_verbose=detection["value_verbose"],
            fraction=detection["fraction"],
            footprint_id=detection["footprint_id"],
            ver=detection["ver"],
            company=detection["company"]
            )],
            contour=detection["contour"],
            id=region_id
        )
        
        #TODO: once we change frame_annotations into map, no need to check this
        tstamp_exists = False
        t2=detection["t"]
        i=0
        #https://stackoverflow.com/questions/21388026/find-closest-float-in-array-for-all-floats-in-another-array
        for i,image_ann in enumerate(list(self.doc["media_annotation"]["frames_annotation"])):
            t1=image_ann["t"]                                  
            if times_equal(float(t1) , float(t2),eps=t_eps):
                log.debug("Appending new region to existing frame annotation.")
                image_ann["regions"].append(region)
                self.doc["media_annotation"]["frames_annotation"][i]=image_ann
                tstamp_exists = True
                break
            if t1>t2 and not times_equal(float(t1), float(t2),eps=t_eps):#redundant I know. Just in case we remove the equality check from lines above
                break
        if not tstamp_exists:
            log.debug("Creating new frame annotation with tstamp: {}".format(detection["t"]))
            image_ann = create_image_ann(
                t=detection["t"],
                regions=[region]
                )
            self.doc["media_annotation"]["frames_annotation"].insert(i,image_ann)
    def append_image_anns(self, image_anns):
        for image_ann in image_anns:
            self.append_image_ann(image_ann)

    def append_image_ann(self, image_ann):
        self.doc["media_annotation"]["frames_annotation"].append(image_ann)

    def append_region_to_image_ann(self, region, tstamp):
        #Potentially change frames_annotations into a map for O(1) lookup in the future
        for image_ann in self.doc["media_annotation"]["frames_annotation"]:
            if image_ann["t"] == tstamp:
                image_ann["regions"].append(region)

    def append_track_to_tracks_summary(self,track):
        #log.info(pprint.pformat(track))
        if 'array' in self.doc["media_annotation"]["tracks_summary"]:
            self.doc["media_annotation"]["tracks_summary"]["array"].append(track)
        else:
            self.doc["media_annotation"]["tracks_summary"].append(track)            
            #log.info("len(self.doc['media_annotation']['tracks_summary']['array']): " + str(len(self.doc["media_annotation"]["tracks_summary"]["array"])))

    def append_annotation_tasks(self, annotation_tasks):
        for at in annotation_tasks:
            self.append_annotation_task(at)

    def append_annotation_task(self, annotation_task):
        self.doc["media_annotation"]["annotation_tasks"].append(annotation_task)

    def sort_image_anns_by_timestamp(self):
        tmp = self.doc["media_annotation"]["frames_annotation"]
        self.doc["media_annotation"]["frames_annotation"] = sorted(tmp, key=lambda k: k['t'])

    def sort_tracks_summary_by_timestamp(self):
        tmp = self.doc["media_annotation"]["tracks_summary"]
        self.doc["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k['t2'])
        self.doc["media_annotation"]["tracks_summary"] = sorted(tmp, key=lambda k: k['t1'])

    def compute_video_desc(self):
        pass

    """Setters"""
    def set_media_ann(self, media_ann):
        assert(isinstance(media_ann, dict))
        self._clear_maps()
        self.doc["media_annotation"] = media_ann

    def set_url(self, url):
        assert(isinstance(url, str))
        self.doc["media_annotation"]["url"] = url

    def set_url_original(self, url):
        assert(isinstance(url, str))
        self.doc["media_annotation"]["url_original"] = url

    def set_dims(self, w, h):
        assert(isinstance(w, int))
        assert(isinstance(h, int))
        self.doc["media_annotation"]["w"] = w
        self.doc["media_annotation"]["h"] = h

    def set_footprints(self, codes):
        assert(isinstance(codes, list))
        self.doc["media_annotation"]["codes"] = codes

    """Getters"""

    def get_servers(self):
        """Get all servers from avro doc

        Returns:
            list (str): list of servers
        """
        servers = []
        for code in self.doc["media_annotation"]["codes"]:
            servers.append(code["server"])
        return servers

    def get_codes(self):
        """Get all footprints from avro doc

        Returns:
            list (dict): list of footprints
        """
        return self.doc["media_annotation"]["codes"]

    def get_segments_from_tracks_summary(self):
        """Transform all tracks_summary to a list of segements

        Returns:
            list[dict]: list of segments
        """
        all_segments = []
        for track in self.get_tracks():
            t1 = track["t1"]
            t2 = track["t2"]
            region_ids = track["region_ids"]
            
            # Track ID isn't a current schema field
            if track.get("track_id") is None:
                track_id = str(datetime.now())
            else:
                track_id = track["track_id"]

            for prop in track["props"]:
                seg = create_segment(
                        server=prop["server"],
                        property_type=prop["property_type"],
                        value=prop["value"],
                        value_verbose=prop["value_verbose"],
                        confidence=prop["confidence"],
                        fraction=prop["fraction"],
                        t1=t1,
                        t2=t2,
                        version=prop["ver"],
                        property_id=prop["property_id"],
                        region_ids=region_ids,
                        track_id=track_id,
                        company=prop["company"]
                    )
                all_segments.append(seg)
        return all_segments

    def get_detections_from_frame_anns(self):
        """Transform all frames_annotations to a list of detections 

        Returns:
            list[dict]: list of detections sorted by t
        """
        all_detections = []
        for image_ann in self.get_image_anns():
            tstamp = image_ann["t"]
            for region in image_ann["regions"]:
                for prop in region["props"]:
                    det = create_detection(
                        server=prop["server"],
                        module_id=prop["module_id"],
                        property_type=prop["property_type"],
                        value=prop["value"],
                        value_verbose=prop["value_verbose"],
                        confidence=prop["confidence"],
                        fraction=prop["fraction"],
                        company=prop["company"],
                        t=tstamp,
                        contour=region["contour"],
                        ver=prop["ver"],
                        property_id=prop["property_id"],
                        region_id=region["id"],
                        footprint_id=prop["footprint_id"]
                    )
                    all_detections.append(det)
            # all_detections.append(single_frame_detections)
        return all_detections


    def get_detections_from_props(self, props):
        """Given a list of frames_ann properties get the corresponding detections where one property is fully met

        Args:
            props: A dict, or list of dicts, of detection key/value pairs

        Returns:
            list[dict]: list of detections matching one of the props           
        """
        if type(props) is dict:
            props = [props]

        if self.detection_querier is None:
            self.build_detection_querier()

        Q = AvroQueryBlock()
        Q.set_operation("OR")
        for prop in props:
            q = AvroQuery()
            q.set(prop)
            Q.add(q)

        return self.detection_querier.query(Q)

    def get_detections_grouped_by_region_id(self, *props):
        """Given a list of frames_ann properties get the corresponding detections where one property is fully met

        Args:
            props (list): Any number of lists of detection key/value pairs

        Returns:
            list[tuple]: A list of grouped detections          
        """
        props = list(props)
        for idx, prop in enumerate(props):
            if type(prop) is dict:
                props[idx] = [prop]

        if self.detection_querier is None:
            self.build_detection_querier()

        Qs = []
        for prop_list in props:
            Q = AvroQueryBlock()
            Q.set_operation("OR")
            for prop in prop_list:
                q = AvroQuery()
                q.set(prop)
                Q.add(q)
            Qs.append(Q)
        dets=[]
        try:
            dets=self.detection_querier.group_query(Qs, "region_id")
        except Exception as e:
            log.warning(e)
            log.info("No detections were found.")
            log.info(traceback.print_exc())
        return dets

    def get_unique_detection_property_groups_by_region_id(self, *props, keys=None):
        """Given two lists of frames_ann properties get the unique combinations of given keys

        Args:
            props (list): Any number of lists of detection key/value pairs
            keys (any iterable): An iterable defining the which keys to define unique-ness against

        Returns:
            list[tuple]: A list of unique combinations defined by keys
        """
        results = self.get_detections_grouped_by_region_id(*props)

        if keys is None:
            keys = results[0].keys()

        filtered_results = sorted([frozenset([json.dumps({key:d[key] for key in keys}, sort_keys=True) for d in dict_sets]) for dict_sets in results])

        # filtered_results = np.array(filtered_results)
        # unique_results = np.unique(filtered_results)
        # unique_results = unique_results.tolist()
        unique_results = set(filtered_results)
        unique_results = [tuple([json.loads(d) for d in dict_set]) for dict_set in unique_results]
        return unique_results
    
    def get_detections_from_frame_anns_t_p(self,tstamp,p):
        """Transform all frames_annotations for a certain timestamp and property of interest to a list of a list of detections
        
        A replica of vision_server/CAvroDevice.hpp::getImagesDetections. 

        Returns:
            list[list[dict]]: outer list corresponds to single tstamp frame, inner list is a list of regions for that tstamp
        """
        image_ann=self.get_image_ann_from_t(tstamp)
        single_frame_detections = []
        if not image_ann:
            return single_frame_detections        
        for region in image_ann["regions"]:
            for prop in region["props"]:
                if not partial_value_AND_match(prop, p):
                    continue
                det = create_detection(
                    server=prop["server"],
                    module_id=detection["module_id"],
                    property_type=prop["property_type"],
                    value=prop["value"],
                    value_verbose=prop["value_verbose"],
                    confidence=prop["confidence"],
                    fraction=prop["fraction"],
                    t=tstamp,
                    contour=region["contour"],
                    ver=prop["ver"],
                    property_id=prop["property_id"],
                    region_id=region["id"]
                )
                single_frame_detections.append(det)            
        return single_frame_detections
    
    def get_image_anns(self):
        """Get all frame annotations

        Returns:
            list of image_ann objects
        """
        return self.doc["media_annotation"]["frames_annotation"]

    def get_media_summaries(self):
        """Get full media summary

        Returns:
            list of media_summary objects
        """
        return self.doc["media_annotation"]["media_summary"]

    def get_footprints(self):
        """Get all footprints

        Returns:
            list of footprints
        """
        return self.doc["media_annotation"]["codes"]

    def get_footprint(self, query_footprint):
        """Get all footprints from doc that match query_footprint with AND logic
        i.e. only if all fields in query_footprint are a match, returns footprints
        Note that in the case of dates, we'll be awaiting date_min date_max and we'll look for the footprints within that range.
        Args:
            query_footprint (dict): footprint to be matched

        Returns:
            footprints (list): list of footprints.
        """
        codes=[]
        for c in self.doc["media_annotation"]["codes"]:
            if partial_value_AND_match(c, query_footprint) and date_range_match(c, query_footprint):
                codes.append(c)
        return codes

    def get_image_anns_from_prop(self, query_prop):
        """Get all image annotations from doc that match query_prop with AND logic
        i.e. only if all fields in query_prop are a match, returns regions

        Args:
            query_prop (dict): properties to be matched

        Returns:
            regions (list): list of dictionaries of regions
        """
        image_anns = []
        for image_ann in self.doc["media_annotation"]["frames_annotation"]:
            regions = []
            for region in image_ann["regions"]:
                for prop in region["props"]:
                    if partial_value_AND_match(prop, query_prop):
                        regions.append(region)
            if regions:
                image_anns.append({"t": image_ann["t"], "regions": regions})
        return image_anns

    def get_regions_from_prop(self, query_prop):
        """Get all regions from doc that match query_prop with AND logic
        i.e. only if all fields in query_prop are a match, returns regions

        Args:
            query_prop (dict): properties to be matched

        Returns:
            regions (list): list of dictionaries of regions
        """
        regions = []
        ts=[]
        for image_ann in self.doc["media_annotation"]["frames_annotation"]:
            t=image_ann['t']
            for region in image_ann["regions"]:
                for prop in region["props"]:
                    if partial_value_AND_match(prop, query_prop):
                        regions.append(region)
                        ts.append(t)
        return regions,ts

    def get_image_ann_from_t(self, t):
        """Get image ann from a specific timestamp, t

        Args:
            t (float): timestamp
        
        Returns:
            image_ann (dict): image annotation (cv schema), None if there is 
        """
        if not self._t2index:
            self.build_t2index_map()
        i = self._t2index.get(t)
        if i is None:
            return None
        return self.doc["media_annotation"]["frames_annotation"][i]

    def get_url(self):
        return self.doc["media_annotation"]["url"]

    def get_dims(self):
        return (self.get_width(), self.get_height())

    def get_width(self):
        return self.doc["media_annotation"]["w"]

    def get_height(self):
        return self.doc["media_annotation"]["h"]

    def get_response(self):
        return self.doc

    def get_json(self, indent=None):
        return json.dumps(self.doc, indent=indent)

    def get_detector_id(self):
        pass

    def get_module_id(self):
        pass

    def get_detector_version(self):
        pass

    def get_detector_min_conf(self):
        pass

    def get_labels(self):
        pass

    def get_timestamps(self):
        return sorted([x['t'] for x in self.doc["media_annotation"]["frames_annotation"]])
    
    def get_timestamps_from_footprints(self,server=None):
        tstamps=[]        
        for c in self.doc["media_annotation"]["codes"]:
            log.info(str(c))
            if not c["tstamps"]:
                continue
            if server:
                if c["server"] != server:
                    continue
            if not tstamps:
                log.info("Assigning timestamps: " + str(c["tstamps"]))
                tstamps=c["tstamps"]
            else:
                tstamps=list(set(tstamps) & set(c["tstamps"])) 
        return tstamps
    def get_annotator(self):
        pass

    def get_tracks(self):
        return self.doc["media_annotation"]["tracks_summary"]
    
    def get_tracks_from_label(self, label):
        input("NEEDS TO BE REVIEWED")
        tracks = []
        for track in self.doc["media_annotation"]["tracks_summary"]:
            if track["regions"][0]["props"][0]["value"] == label:
                tracks.append(track)
        return tracks

    def get_tracks_from_prop(self,pois):
        log.info("Getting tracks from props:")
        for p in pois:
            log.info(str(p))
        log.info("-------------")
        #input()
        tracks =[]
        confidences=[]
        #we go through the tracks, and we check whether they show the poi or not.
        #log.info(str(self.doc))
        tracks_summary=self.doc["media_annotation"]["tracks_summary"]
        #log.info("We got a track summary, type: " + str(type(tracks_summary)))
        #log.info(str(tracks_summary.keys()))
        #tracks_summary=tracks_summary["array"]
        log.info("len(tracks_summary): " + str(len(tracks_summary)))
        #log.info(pprint.pformat(poi))
        for t in tracks_summary:
            #log.info("We got a track, type: " + str(type(t)))
            toi_flag=False
            props=t["props"]
            n_props_present=0
            confidence_local=1
            for p in props:                
                #for all the props in poi we need a true
                #log.info("p: " + str(p))
                for poi in pois:
                    #log.info("poi: " + str(poi))
                    if partial_value_AND_match(p,poi):
                        #toi_flag=True
                        n_props_present+=1
                        if p["confidence"] < confidence_local:
                            confidence_local=p["confidence"]
           # log.info("-------------")                
           # input()
            if n_props_present==len(pois):
                toi_flag=True
            if toi_flag:
                tracks.append(t)
                confidences.append(confidence_local)
        log.info("len(tracks): " + str(len(tracks)))
        log.info("len(confidences): " + str(len(confidences)))
        return tracks,confidences

    def get_tracks_from_timestamp(self, tstamp):
        tracks = []
        for track in self.doc["media_annotation"]["tracks_summary"]:
            if track['t1'] < tstamp < track['t2']:
                tracks.append(track)
        return track

    def get_sorted_tracks_from_timestamp(self, tstamp):
        tracks = []
        found = False
        for track in self.doc["media_annotation"]["tracks_summary"]:
            if track['t1'] < tstamp:
                found = True
                if tstamp < track['t2']:
                    tracks.append(track)
            elif found:
                break
        return tracks

    def get_temporal_signal_from_prop(self,poi,use_track_summary_flag=True,tstamps_msecs=[]):
        if use_track_summary_flag==False:
            log.info("Retrieving temporal signal from frame annotations")
            tstamps_secs=self.get_timestamps()
            tstamps_msecs = [int(1000*x) for x in tstamps_secs]
            s=[np.array([]) for c in range(len(tstamps_msecs))]
            for i,tstamp in enumerate(tstamps_secs):
                #if i>=1000:
                #    break
                if i%100==0:
                    log.debug("tstamp: " + str(tstamp))
                dets=self.get_detections_from_frame_anns_t_p(tstamp,poi)
                for d in dets:
                    conf=d['confidence']
                    #log.info("Appending in i:" + str(i) +" conf:"+str(conf))
                    s[i]=np.append(s[i],conf)
            return s, tstamps_msecs


        log.info("Retrieving temporal signal from tracks")
        #We retrieve the tracks from the track_summary
        tracks,confidences=self.get_tracks_from_prop(poi)
        #we create a temporal signal with the temporal coordinates and the confidences.
        s=[]
        tstamps=tstamps_msecs
        #tstamps=get_timestamps_msecs()        
        if len(tstamps)==0:
            #log.warning("Setting default timestamps, 25fps, T=40msecs")
            #tstamps=range(0,4*3600*1000,40)
            log.warning("Setting default timestamps, 2fps, T=500msecs")
            max_video_hours=3
            Tmsec=1000
            tstamps=range(0,max_video_hours*3600*1000,Tmsec)
        #s=np.zeros(len(tstamps))       
        s=[np.array([]) for c in range(len(tstamps))]
    
        #we go through the properties and we update the signal
        tmax=0
        for track,conf in zip(tracks,confidences):
            #we get t1 and t2
            t1=track["t1"]
            t2=track["t2"]
            #log.info("track: " + str(t1) +", " +str(t2)+", " + str(conf))
            #we update tmax
            if t2>tmax:
                tmax=t2
            #we update the signal with the average confidence.
            i1 = (np.abs(np.array(tstamps,dtype=np.uint64)/1000.0-t1)).argmin()
            i2 = (np.abs(np.array(tstamps,dtype=np.uint64)/1000.0-t2)).argmin()
            #log.info("i1|t1: " + str(i1) + "|" + str(t1) +", " + str(tstamps[i1]))
            #log.info("i2|t2: " + str(i2) + "|" + str(t2) +", " + str(tstamps[i2]))
            for i in range(i1,i2):
                #log.info("Appending in i:" + str(i) +" conf:"+str(conf))
                s[i]=np.append(s[i],conf)
        log.info("Returning signal.")
        return s,tstamps

    def get_detections(self):
        if self.detections is None:
            self.build_detections_from_frames_ann()
        return self.detections

    def get_detections_from_region_id(self, region_id):
        """Query for detections with a specific region_id

        Args:
            region_id (str): A detection region_id

        Returns:
            list: A list of detections
        """
        if self.detection_querier is None:
            self.build_detection_querier()

        q = AvroQuery()
        q.match_region_id(region_id)

        return self.detection_querier.query(q)

    def get_region_ids_from_t(self, t):
        if not self._t2region_ids:
            self.build_t2region_id_map()
        return self._t2region_ids[t]
    
    def get_detections_from_t(self, t):
        dets = []
        for region_id in self.get_region_ids_from_t(t):
            dets=dets+self._region_id2detections[region_id]
        return dets

    def get_matching_detections(self, **kwargs):
        """Queries for detections with matching key/value pairs.

        Keyword arguments that match keys in detections will be treated as exact matches. Refer to cv_shcema_factory.

        Special Keyword Args:
            t1 (float): Will match all segments where t1 is less than t
            t2 (float): Will match all segments where t2 greater than t

        Returns:
            List of matching detections

        TODOs:
            1) Add range query for confidence
        """
        if getattr(self, "frames_ann_query_map", None) is None:
            self.build_frames_ann_query_map()
        desired_idxs = self._get_desired_indicies(self.frames_ann_query_map, kwargs)
        return self.detections[sorted(desired_idxs)].tolist()

    def get_matching_track_segments(self, **kwargs):
        """Queries for segments with matching key/value pairs.

        Keyword arguments that match keys in segments will be treated as exact matches. Refer to cv_shcema_factory.

        Special Keyword Args:
            t (float): Will match all segments where t is an element of t1 to t2, exclusive

        Returns:
            List of matching detections

        TODOs:
            1) Add range query for confidence
        """
        if getattr(self, "track_summary_query_map", None) is None:
            self.build_track_summary_query_map()
        desired_idxs = self._get_desired_indicies(self.track_summary_query_map, kwargs)
        return self.track_segments[sorted(desired_idxs)].tolist()

    def _get_desired_indicies(self, query_map, query):
        """Hidden function to query the query_map for the relevant detection or segement indicies

        Args:
            query_map (dict): A dict of dicts of sets described in build_frames_ann_query_map or build_tracks_summary_query_map
            query (dict): A dict of key/value pairs that match fields in segments and detections. There are some special keys described in get_matching_track_segments and get_matching_detections

        Returns:
            indicies (set): A set of the unique indices of segments/detections that match the query

        """
        desired_idxs = None

        def intersect(desired_idxs, key, value):
            if desired_idxs is None:
                desired_idxs = query_map[key][value]
                return desired_idxs
            desired_idxs = desired_idxs.intersection(query_map[key][value])
            return desired_idxs

        def timestamp_in_range_query(query_map, value):
            if self.gpu is not None:
                with cp.cuda.Device(self.gpu):
                    idxs = cp.nonzero(cp.logical_and(query_map["t1"] < value, query_map["t2"] > value))[0].tolist()
            else:
                idxs = np.nonzero(np.logical_and(query_map["t1"] < value, query_map["t2"] > value))[0].tolist()
            return set(idxs)

        def range_around_timestamp_query(query_map, t1=None, t2=None):
            t1_idxs = None
            t2_idxs = None
            if self.gpu is not None:
                with cp.cuda.Device(self.gpu):
                    t = cp.array(query_map["t_list"])
            else:
                t = query_map["t_list"]

            if t1 is not None:
                t1_idxs = t > t1
            if t2 is not None:
                t2_idxs = t < t2

            if t1_idxs is not None and t2_idxs is not None:
                if self.gpu is not None:
                    with cp.cuda.Device(self.gpu):
                        idxs = cp.nonzero(cp.logical_and(t1_idxs, t2_idxs))[0].tolist()
                else:
                    idxs = np.nonzero(np.logical_and(t1_idxs, t2_idxs))[0].tolist()

            elif t1_idxs is not None:
                if self.gpu is not None:
                    with cp.cuda.Device(self.gpu):
                        idxs = cp.nonzero(t1_idxs)[0].tolist()
                else:
                    idxs = np.nonzero(t1_idxs)[0].tolist()

            elif t2_idxs is not None:
                if self.gpu is not None:
                    with cp.cuda.Device(self.gpu):
                        idxs = cp.nonzero(t2_idxs)[0].tolist()
                else:
                    idxs = np.nonzero(t1_idxs)[0].tolist()
            else:
                idxs = t.tolist()

            return set(idxs)


        skip_t1 = False
        skip_t2 = False
        for key, value in query.items():
            if key is "t" and query_map.get("t1") is not None:
                idxs = timestamp_in_range_query(query_map, value)
                if desired_idxs is None:
                    desired_idxs = idxs
                else:
                    desired_idxs = desired_idxs.intersection(idxs)
                continue
            
            if (key is "t1" and skip_t1) or (key is "t2" and skip_t2):
                continue

            if key is "t1" and query_map.get("t") is not None:
                skip_t2 = True
                idxs = range_around_timestamp_query(query_map, value, query.get("t2"))
                if desired_idxs is None:
                    desired_idxs = idxs
                else:
                    desired_idxs = desired_idxs.intersection(idxs)
                continue

            if key is "t2" and query_map.get("t") is not None:
                skip_t1 = True
                idxs = range_around_timestamp_query(query_map, query.get("t1"), value)
                if desired_idxs is None:
                    desired_idxs = idxs
                else:
                    desired_idxs = desired_idxs.intersection(idxs)
                continue

            if query_map.get(key) is None:
                log.warning("Key Not Found: {}... Skipping".format(key))
                continue

            if query_map[key].get(value) is None:
                log.warning("Value for Key Not Found: {{{}: {}}}... Skipping".format(key, value))
                continue

            if type(value) is list:
                for elem in value:
                    desired_idxs = intersect(desired_idxs, key, elem)
            else:
                desired_idxs = intersect(desired_idxs, key, value)
        
        if desired_idxs is None:
            desired_idxs = set()

        return desired_idxs

    
    def get_image_ann_from_region_id(self, region_id):
        # 
        # OBSOLETE
        # 
        for image_ann in self.doc["media_annotation"]["frames_annotation"]:
            for region in image_ann['regions']:
                if region['id'] == region_id:
                    return image_ann
        return None

    def get_region_from_region_id(self, region_id):
        # 
        # OBSOLETE
        # 
        for image_ann in self.doc["media_annotation"]["frames_annotation"]:
            for region in image_ann["regions"]:
                if region["id"] == region_id:
                    return region
        return None

    def get_region_from_region_ids_and_tstamp(self, region_ids, tstamp):
        # 
        # OBSOLETE
        # 
        image_ann = self.get_image_ann_from_t(tstamp)
        if image_ann:
            for region in image_ann["regions"]:
                if region["id"] in region_ids:
                    return region
        return None

    """Builders"""  
    def build_detection_querier(self):
        log.info("Building AvroQuerier")
        self.detection_querier = AvroQuerier()
        log.info("Getting detections")
        dets=self.get_detections_from_frame_anns()
        self.detection_querier.load(dets)

    def build_segment_querier(self):
        self.segment_querier = AvroQuerier()
        self.segment_querier.load(self.get_segments_from_tracks_summary())

    def _clear_maps(self):
        self._region_id2detections = {}
        self._t2region_ids = {}
        self._t2index = {}

    def build_detections_from_frames_ann(self):
        self.detections = np.array(self.get_detections_from_frame_anns())

    def build_segments_from_tracks(self):
        self.track_segments = np.array(self.get_segments_from_tracks_summary())

    def build_region_id2detection_map(self):
        # 
        # OBSOLETE
        # 
        if self.detections is None:
            self.build_detections_from_frames_ann()

        for det in self.detections:
            if not self._region_id2detections.get(det['region_id']):
                self._region_id2detections[det['region_id']] = []
            self._region_id2detections[det['region_id']].append(det)
    
    def build_t2region_id_map(self):
        # 
        # OBSOLETE
        # 
        if self.detections is None:
            self.build_detections_from_frames_ann()

        for dets_at_t in self.dets:
            t = dets_at_t[0]['t']
            self._t2region_ids[t] = set()
            for det in dets_at_t:
                self._t2region_ids[t].add(det['region_id'])
    
    def build_t2index_map(self):
        # 
        # OBSOLETE
        # 
        self._t2index={}
        for i,img_ann in enumerate(self.doc["media_annotation"]["frames_annotation"]):
            self._t2index[img_ann['t']]=i

    def build_frames_ann_query_map(self):
        """Builds a python dict to quickly query for detections from frames annotation.

        Philosphy:
        Every detection is made up of a bunch of `key`, `value` pairs, and has a specific index in a list. The query_map is a dict of dicts of sets. query_map[`key`][`value`] returns a set of ints corresponding the indicies of the matching placements.
        
        TODOs:
        1) Add query for contour size
        2) Add query for confidence range
        """
        query_map = {}
        if self.detections is None:
            self.build_detections_from_frames_ann()

        unsupported = set(["contour"])

        query_map["t_list"] = np.array([])
        
        for idx, det in enumerate(self.detections):
            for key in det.keys():
                val = det[key]
                if key in unsupported:
                    continue

                if key is "t":                    
                    query_map["t_list"] = np.append(query_map["t_list"], idx)

                if key not in query_map.keys():
                    query_map[key] = {}
                if val not in query_map[key].keys():
                    query_map[key][val] = set()
                query_map[key][val].add(idx)

        if self.gpu is not None:
            with cp.cuda.Device(self.gpu):
                query_map["t_list"] = cp.array(query_map["t_list"])

        self.frames_ann_query_map = query_map

    def build_track_summary_query_map(self):
        """Builds a python dict to quickly query for segments from tracks summary.

        Philosphy:
        Every detection is made up of a bunch of `key`, `value` pairs, and has a specific index in a list. The query_map is a dict of dicts of sets. query_map[`key`][`value`] returns a set of ints corresponding the indicies of the matching placements.
        
        TODOs:
        1) Add query for confidence range
        """
        query_map = {}
        if getattr(self, "track_segments", None) is None:
            self.build_segments_from_tracks()

        for idx, segment in enumerate(self.track_segments):
            for key in segment.keys():
                val = segment[key]

                if key not in query_map.keys():
                    query_map[key] = {}

                if key is "t1" or key is "t2":
                    if type(query_map[key]) is dict:
                        query_map[key] = np.array([])
                    query_map[key] = np.append(query_map[key], val)
                    continue

                if type(val) is not list:
                    val = [val]

                for v in val:
                    if v not in query_map[key].keys():
                        query_map[key][v] = set()

                    query_map[key][v].add(idx)

        if self.gpu is not None:
            with cp.cuda.Device(self.gpu):
                if query_map.get("t1") is not None:
                    query_map["t1"] = cp.array(query_map["t1"])
                if query_map.get("t2") is not None:
                    query_map["t2"] = cp.array(query_map["t2"])

        self.track_summary_query_map = query_map
            
def date_range_match(item, query):
    if int(query["date_min"]) > int(item["date"]):
        return False
    if int(query["date_max"]) < int(item["date"]):
        return False
    return True

def partial_value_AND_match(item, query_item):
    """Boolean test to see if each key/value in `query_item` matches in itemself.
    Skips comparision if default values
    """
    if type(query_item) == type([]):
        for qitem in query_item:
            if partial_value_AND_match(item, qitem):
                return True
        return False
    else:
        for qkey, qvals in query_item.items():
            if qvals == 0.0 or qvals == "" or qvals == []:
                continue
            if qkey not in item.keys():
                continue
            if item[qkey] != qvals:
                return False
        return True

