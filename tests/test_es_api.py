import context
from vitamincv.comm_apis.es_api import ESAPI, log
from vitamincv.avro_api import avro_api
import pprint


footprints = []
props = []

p = {}
p["confidence_min"] = 0.9
p["confidence_max"] = 1.0
p["company"] = "gumgum"
p["server"] = "FaceDetector"
p["ver"] = ""
p["property_type"] = "label"
p["value"] = "face"
props = [p]

f = {}
f["ver"] = ""
f["company"] = "gumgum"
f["code"] = "SUCCESS"
f["server"] = "FaceDetector"
f["date_min"] = 20180515084537
f["date_max"] = 20180515084539
footprints = [f]


f5 = {}
# f["ver"] = ""
f5["company"] = "gumgum-sports"
f5["code"] = "SUCCESS"
f5["server"] = "HAM"
f5["date_min"] = 20180505013746
f5["date_max"] = 20180505013748
footprints5 = [f5]

f6 = {}
# f6["ver"] = ""
f6["company"] = "gumgum-sports"
f6["code"] = "SUCCESS"
f6["server"] = "HAM"
f6["date_min"] = 20180509184247
f6["date_max"] = 20180509184249
footprints6 = [f6]

f7 = {}
# f7["ver"] = ""
f7["company"] = "gumgum-sports"
f7["code"] = "SUCCESS"
f7["server"] = "HAM"
f7["date_min"] = 20180510000000  # 20180509000000
f7["date_max"] = 20180511000000  # 20180510000000
footprints7 = [f7]


p2 = {}
p2["confidence_min"] = 0.9
p2["confidence_max"] = 1.0
p2["company"] = "gumgum"
p2["server"] = "HAM"
p2["ver"] = ""
p2["property_type"] = "sponsor_name"
# p2["value"] = "Nike" #44279
# p2["value"] = "StateFarm" #2634
# p2["value"] = "PlayStation"#424
# p2["value"] = "Squarespace" #5768
# p2["value"] = "UniCredit" #32
# p2["value"] = "Adidas" #4839
# p2["value"] = "CoorsLight" #231
# p2["value"] = "Honda" #1605
p2["value"] = "Spalding"  # 780
props2 = [p2]


p3 = {}
p3["confidence_min"] = 0.75
p3["confidence_max"] = 1.0
p3["company"] = "gumgum"
p3["server"] = "SportClassifier"
p3["ver"] = ""
p3["property_type"] = "sport"
p3["value"] = "counter"  # 3600
props3 = [p3]


p4 = {}
p4["confidence_min"] = 0.975
p4["confidence_max"] = 1.0
# p4["company"] = "gumgum"
p4["server"] = "LeagueClassifier"
# p4["ver"] = ""
p4["property_type"] = "league"
p4["value"] = "counter"  # 18821
props4 = [p4]


def test_es_api():
    n = 10
    x = ESAPI()
    x.set(footprints, props)
    docs = x.pull(n)
    log.info("len(docs): " + str(len(docs)))

    # we check the retrieved doc has the properties we are looking for
    xavro = avro_api.AvroAPI()
    for d in docs:
        # pprint(d["_source"])
        xavro.set_media_ann(d)
        print(p)
        regions = xavro.get_regions_from_prop(p)
        log.info("********************")
        log.info("Printing " + str(len(regions)) + " regions.")

        for r in regions:
            log.info(pprint.pformat(r))
            codes = xavro.get_footprint(f)
            log.info("********************")
            log.info("Printing " + str(len(codes)) + " codes.")
        for c in codes:
            log.info(pprint.pformat(c))
    assert len(regions) > 0
    assert len(codes) > 0


def test_es_api_count_1():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set([], props)
    count = x.count()
    print("Count=" + str(count))
    assert count > 0


def test_es_api_count_2():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set([], props2)
    count = x.count()
    print("Count=" + str(count))
    assert count > 0


def test_es_api_count_3():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set([], props3)
    count = x.count()
    print("Count=" + str(count))
    assert count > 0


def test_es_api_count_4():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set([], props4)
    count = x.count()
    print("Count=" + str(count))
    assert count > 0


def test_es_api_count_5():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set(footprints5, [])
    count = x.count()
    print("Count=" + str(count))
    assert count > 0
    docs = x.pull(1)
    log.info("len(docs): " + str(len(docs)))
    assert len(docs) > 0
    for d in docs:
        print(d)


def test_es_api_count_6():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set(footprints6, [])
    count = x.count()
    print("Count=" + str(count))
    assert count > 0
    docs = x.pull(1)
    log.info("len(docs): " + str(len(docs)))
    assert len(docs) > 0
    for d in docs:
        print(d)


def test_es_api_count_7():
    print("********************")
    print("Testing count query")
    x = ESAPI()
    x.set(footprints7, [])
    count = x.count()
    print("Count=" + str(count))
    assert count > 0
    docs = x.pull(1)
    log.info("len(docs): " + str(len(docs)))
    assert len(docs) > 0
    for d in docs:
        print(d)
