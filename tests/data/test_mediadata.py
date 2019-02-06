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
import pandas as pd

import numpy as np
def t():
    dets = [
        {'property_type': 'placement', 'value': 'board', 't': 1}, 
        {'property_type': 'placement', 'value': 'led', 't': 2}, 
        {'property_type': 'logo', 'value': 'AAA', 't': 1},
        {'property_type': 'logo', 'value': 'statefarm', 't': 2},
        {'property_type': 'placement', 'value': 'board', 't': 3}, 
        {'property_type': 'logo', 'value': 'AAA', 't': 3},
        {'property_type': 'hfe', 'value': 'AAA', 't': 3}
        ]
    
    query = [
        {'property_type': 'placement', 't': 3},
        {'property_type': 'placement', 't': 1}
    ]

    qstr = ""
    for i, q in enumerate(query):
        for j, (k, v) in enumerate(q.items()):
            qstr += f'({k} == "{v}")'
            if j != len(q)-1:
                qstr += " & "
        if i != len(query)-1:
            qstr += " | "
    print(qstr)

    # query = {'property_type': 'placement', 't': 3}
    
    df = pd.DataFrame(dets)
    # q = {'property_type': 'placement', 't': 3}
    # q2 = {'property_type': 'placement', 't': 1}

    # s1 = (df[list(q)] == pd.Series(q)).all(axis=1)
    # s2 = (df[list(q2)] == pd.Series(q2)).all(axis=1)

    # print(s1)
    # print(s2)
    # v = s1 | s2
    # print(v)
    # print(df[v])

    
    # print(df.loc[np.all(df[list(q)] == pd.Series(q), axis=1)])

    # xstr = f'{q["property_type"]} == {q["t"]}'
    # # print(xstr)
    # print(df)
    # af = "placement"
    # # res = df.query('property_type  == @af')
    # # res = df.query(' (property_type == "placement") | (property_type == "logo") ')
    # # res = df.query("""(property_type == "placement") & (t == "3") | (property_type == "placement") & (t == "1")""")
    # res = df.query(qstr)
    # print(res)
    # # filterSeries = pd.Series(np.ones(df.shape[0],dtype=bool))
    # s = pd.Series()
    # for q in query:
    #     print('s')
    #     print(s)
    #     tmp = (df[list(q)] == pd.Series(q)).all(axis=1)
    #     print('tmp')
    #     print(tmp)
    #     s = s.add(tmp)
    # print('end')
    # print(s)
    # print(type(s))
    # # v = df[(df[list(x)] == pd.Series(x)).all(axis=1)]
    # print(v.to_dict('records'))

    # print(df.groupby(['t']).groups)

    # newdf = df[(df["property_type"] == "placement") | (df["property_type"] == "logo")]
    # print(newdf)
    # print(json.dumps(newdf.to_dict('records'), indent=2))
    groupings = df.groupby(['t']).groups
    print(groupings)
    for k, group in groupings.items():
        print(k)
        print(df.iloc[group].to_dict('records'))
t()
