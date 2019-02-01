import os
import sys
import argparse

from vitamincv.avro_api.avro_api import AvroAPI
from vitamincv.avro_api.avro_io import AvroIO

header = "Broadcast ID,Rightsholder,Sponsor,Placement,Duration,Broadcast Media URL, Start Timestamp, End Timestamp"


def doc_to_csv(json_doc, out_csv, rightsholder, broadcast_id, is_binary=False):
    """Convert json document to csv format with headers:

    Broadcast ID,Rightsholder,Sponsor,Placement,Duration,Broadcast Media URL, Start Timestamp, End Timestamp

    Args:
        json_doc (str): path to json document
        out_csv (str): path to write output csv
        is_binary (bool): flag if json_doc is binary
    """
    doc = None
    if is_binary:
        aio = AvroIO()
        doc = aio.decode_file(json_doc)
    else:
        doc = AvroIO.read_json(json_doc)

    aapi = AvroAPI(doc)
    tracks = aapi.get_tracks()
    url = aapi.get_url()

    with open(out_csv, "w") as wf:
        wf.write(header)
        wf.write("\n")
        for track in tracks:
            logo = ""
            placement = ""
            for prop in track.get("props"):
                if prop.get("property_type") == "placement":
                    placement = prop.get("value")
                if prop.get("property_type") == "logo":
                    logo = prop.get("value")
            if logo == "Garbage" or logo == "Messy":
                continue
            wf.write(
                "{},{},{},{},{},{},{},{}\n".format(
                    broadcast_id,
                    rightsholder,
                    logo,
                    placement,
                    int(round(float(track["t2"]) - float(track["t1"]))),
                    url,
                    track["t1"],
                    track["t2"],
                )
            )


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--doc", required=True, help="input response doc")
    a.add_argument("--is_binary", action="store_true", default=False)
    a.add_argument("--broadcast_id", default=-1)
    a.add_argument("--rightsholder", default="No-RH")
    a.add_argument("--out_csv", required=True, help="output csv file")
    args = a.parse_args()

    doc_to_csv(args.doc, args.out_csv, args.rightsholder, int(args.broadcast_id), args.is_binary)
