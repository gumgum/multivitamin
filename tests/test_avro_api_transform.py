import context
from vitamincv.avro_api.avro_api import AvroIO, AvroAPI
import glog as log
import pprint
import json
import glob

s3bucket="/home/fjm/sandbox/production2avro/s3bucket/"



#date="2018-05-11" #TESTED
#date="2018-03-02" #TESTED
#jsons_filepath_20180511=s3bucket + "/" + date +"/"+ "post_detections_export.json"
#schema_filepath="/home/fjm/sandbox/cvapis/avro_api/image-science-response.avsc"
#jsons_filepath=jsons_filepath_20180511

def test_avro_api_transform():
    x=AvroIO()
    schema=x.get_schema()
    json_files=glob.glob(s3bucket + '/**/*.json', recursive=True)
    log.info("len(json_files): " + str(len(json_files)))
    #input()
    for jsons_filepath in json_files:
        log.info("Checking transformation for " + jsons_filepath)
        jsons = json.load(open(jsons_filepath))
        log.info("len(jsons): " + str(len(jsons)))
        for i,j in enumerate(jsons):
            if i%100==0:
                log.info("i: "+str(i))
                json_updated=AvroAPI.transform(j,schema)
                #input()
        log.info("TRANSFORMED DOCUMENTS:  " + str(len(jsons)))
