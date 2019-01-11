import argparse

from vitamincv.avro_api.avro_api import AvroAPI
from vitamincv.avro_api.avro_io import AvroIO
from vitamincv.avro_api.utils import to_SSD_ann_format

if __name__=="__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--avro")
    a.add_argument("--out_dir")
    args = a.parse_args()

    aapi = AvroAPI(AvroIO.read_json(args.avro))
    to_SSD_ann_format(aapi, "placement", args.out_dir)

