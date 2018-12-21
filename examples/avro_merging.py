import context
import argparse
from cvapis.avro_api.cv_schema_factory import *
from cvapis.avro_api.avro_api import AvroIO, AvroAPI

import os
import csv

import glog as log
   
if __name__=="__main__":
    a = argparse.ArgumentParser("python3 avro_merging --list list_pairs.txt")
    a.add_argument("--list_pairs", default="list_pairs.txt", type=str, help="path to a file with pairs of avro docs to bd merged")
    a.add_argument("--dump_dir", default="./merged")
    args = a.parse_args()
    list_pairs=args.list_pairs
    dump_dir=args.dump_dir
    os.makedirs(dump_dir, exist_ok=True)
    with open(list_pairs) as csvfile:#we go thru the list
        a= csv.reader(csvfile,delimiter='\t')
        for row in a:
            f1=row[0]
            f2=row[1]
            bn1=os.path.basename(f1)
            bn2=os.path.basename(f2)
            bn1=os.path.splitext(bn1)[0]
            bn2=os.path.splitext(bn2)[0]
            fout=dump_dir +"/"+ bn2 +"_mergedwithHAM"+ ".json"
            print("---")
            print("f1: " + f1)
            print("f2: " + f2)
            print("fout: " + fout)
            #we merge the two and we dump it
            print("Loading docs")
            r1=AvroAPI(AvroIO.read_json(f1))
            r2=AvroAPI(AvroIO.read_json(f2))
            print("Creating merger")
            merger=AvroAPI()            
            merger.merge(r1,r2)
            AvroIO.write_json(merger.doc, fout, indent=2)

            
    
