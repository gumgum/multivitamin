import json

import pandas as pd
import glog as log


class MediaDataQuerier():
    def __init__(self, media_data):
        self.md = media_data
        self.dets = pd.DataFrame(self.md.detections)

    def get_detections_by_values_of_interest(self, values_of_interest):
        log.info("Filtering detections")
        if not isinstance(values_of_interest, list):
            values_of_interest = [values_of_interest]

        query_str = convert_list_of_query_dicts_to_pd_query(values_of_interest)
        log.info(f"Querying dataframe with query_str: {query_str}")
        return self.dets.query(query_str)


def convert_list_of_query_dicts_to_pd_query(query):
    assert(isinstance(query, list))
    
    qstr = ""
    for i, q in enumerate(query):
        assert(isinstance(q, dict))
        for j, (k, v) in enumerate(q.items()):
            qstr += f'({k} == "{v}")'
            if j != len(q)-1:
                qstr += " & "
        if i != len(query)-1:
            qstr += " | "
    return qstr


def filter_detections_by_properties_of_interest(detections, props_of_interest):
    """Currently, unused"""
    if props_of_interest is None:
        log.warning("props of interest is None, returning all detections")
        return detections

    if isinstance(props_of_interest, dict):
        props_of_interest = [props_of_interest]
    
    log.info(f"Filtering detections given props of interest: {json.dumps(props_of_interest, indent=2)}")
    log.info(f"len(self.detections) before filtering: {len(detections)}")
    for prop in props_of_interest:
        for k, v in prop.items():
            detections = list(filter(lambda det: det.get(k) == v, detections))
    log.info(f"len(self.detections) after filtering: {len(detections)}")
    return detections