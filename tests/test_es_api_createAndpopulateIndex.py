import context
from vitamincv.comm_apis.es_api import ESAPI, log
from vitamincv.avro_api.avro_api import AvroAPI
import pprint
from glob import glob

x=ESAPI()
#if True:
if False:
    log.info("Deleting index: " + x.index)
    input()
    x._delete_index(x.index)
    log.info("Deleted index")
    input()
x=ESAPI()
def transform(json_dict):
    schema_filepath="/home/fjm/sandbox/vitamincv/avro_api/image-science-response.avsc"
    with open(schema_filepath, 'r') as myfile:
        schema=myfile.read()
    return AvroAPI.transform(json_dict,schema)




x.set_document_transform(transform)


human_jsons_root="/home/fjm/sandbox/production2avro/output-export-historical-data"
machine_jsons_root="/home/fjm/sandbox/production2avro/s3bucket/"
log.info("pushing human jsons")
x.push_from_folder(human_jsons_root,num_workers=512)
log.info("pushing machine jsons")
x.push_from_folder(machine_jsons_root,num_workers=512)
