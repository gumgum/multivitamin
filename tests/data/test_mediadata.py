import json

import boto3

from vitamincv.data import create_detection
from vitamincv.data.avro_response import AvroResponse


TEST_BUCKET = "vitamin-cv-test-data"
TEST_DATA = "data/previous_responses/short_flamesatblues.json"


from functools import reduce
from itertools import groupby
from operator import add, itemgetter

def merge_records_by(key, combine):
    """Returns a function that merges two records rec_a and rec_b.
       The records are assumed to have the same value for rec_a[key]
       and rec_b[key].  For all other keys, the values are combined
       using the specified binary operator.
    """
    return lambda rec_a, rec_b: {
        k: rec_a[k] if k == key else combine(rec_a[k], rec_b[k])
        for k in rec_a
    }

def merge_list_of_records_by(key, combine):
    """Returns a function that merges a list of records, grouped by
       the specified key, with values combined using the specified
       binary operator."""
    keyprop = itemgetter(key)
    return lambda lst: [
        reduce(merge_records_by(key, combine), records)
        for _, records in groupby(sorted(lst, key=keyprop), keyprop)
    ]


def init():
    global response
    s3client = boto3.client('s3')
    obj = s3client.get_object(Bucket=TEST_BUCKET, Key=TEST_DATA)
    j = json.loads(obj['Body'].read())
    response = AvroResponse(j)


def test_create_detections():
    init()
    prop1 = {"property_type": "placement"}
    prop2 = {"property_type": "value"}
    props = [prop1, prop2]

    md = response.to_mediadata()
    md.update_maps()
    # print(json.dumps(md.det_regionid_map, indent=2))
    # dets = md.detections
    # # md.filter_detections_by_props(prop)
    # print(f'dets {len(dets)}')

    # for prop in pois:
    #     for k, v in prop.items():
    #         dets = list(filter(lambda det: det.get(k) == v, dets))
    # print(f'dets {len(dets)}')

    _compute_unique_sets(md.det_regionid_map, props, "value")

def _compute_unique_sets(dets_regionid_map, properties_of_interest, keyfunc):
    if isinstance(properties_of_interest, dict):
        properties_of_interest = [properties_of_interest]
    
    uniq_sets = []
    for regionid, dets in dets_regionid_map.items():
        dets_info = []
        for prop in properties_of_interest:
            for key, val in prop.items():
                for det in dets:
                    if det.get(key) == val:
                        dets_info.append(keyfunc(det))
                        # dets_info.append(det.get(key_of_interest))
        # uniq_sets.append(dets_info)
        uniq_sets.append(tuple(dets_info))
    return tuple(uniq_sets)
    # print(json.dumps(uniq_sets, indent=2))
    # print(set(uniq_sets))

def _compute_unique_sets2(dets_regionid_map, properties_of_interest, keyfunc):
    if isinstance(properties_of_interest, dict):
        properties_of_interest = [properties_of_interest]
    
    uniq_sets = []
    for regionid, dets in dets_regionid_map.items():
        dets_info = []
        for prop in properties_of_interest:
            for key, val in prop.items():
                dets_info.append(list(map(keyfunc, filter(lambda det: det.get(key) == val, dets))))
        print(dets_info)

from tinydb import TinyDB, Query, where
import json
def t():
    dets = {
        '1': [
                {'property_type': 'placement', 'value': 'board', 't': 1}, 
                {'property_type': 'logo', 'value': 'AAA', 't': 1},
            ],
         '2': [
                {'property_type': 'placement', 'value': 'led', 't': 2}, 
                {'property_type': 'logo', 'value': 'statefarm', 't': 2},
         ],
         '3': [
                 {'property_type': 'placement', 'value': 'board', 't': 3}, 
            {'property_type': 'logo', 'value': 'AAA', 't': 3}
            
        ]}

    db = TinyDB('tmp.json')
    Dets = Query()
    db.insert(dets)
    print(db.count)
    x = db.search((where('property_type') == 'placement') | (where('property_type') == 'logo'))
    print(json.dumps(x, indent=2))

t()