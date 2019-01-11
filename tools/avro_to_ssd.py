import argparse

from cvapis.avro_api.avro_api import AvroAPI
from cvapis.avro_api.avro_io import AvroIO
from cvapis.avro_api.utils import to_SSD_ann_format

if __name__=="__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--avro")
    a.add_argument("--out_dir")
    a.add_argument("--property_type")
    args = a.parse_args()

    aapi = AvroAPI(AvroIO.read_json(args.avro))
    to_SSD_ann_format(aapi, args.property_type, args.out_dir)

