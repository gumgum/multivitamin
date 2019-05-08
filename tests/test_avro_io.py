import os
import sys
import json

from vitamincv.avro_api.avro_io import AvroIO

local_json = "data/placement.json"

if not os.path.exists(local_json):
    print("Execute pytest test_avro_io.py from within the tests/ folder")
    sys.exit(1)

temp_file = "tmp.avro"


def test_local_encode():
    avroio = AvroIO()
    assert avroio.write(AvroIO.read_json(local_json), temp_file)


def test_local_decode():
    avroio = AvroIO()
    res = avroio.decode_file(temp_file)
    assert True


def test_remote_encode():
    avroio = AvroIO(use_schema_registry=True)
    assert avroio.write(AvroIO.read_json(local_json), temp_file)


def test_remote_decode():
    avroio = AvroIO(use_schema_registry=True)
    doc = avroio.decode_file(temp_file)
    print(json.dumps(doc, indent=2))
    assert True


def test_is_valid_avro_doc():
    avroio = AvroIO()
    doc = avroio.decode_file(temp_file)
    assert avroio.is_valid_avro_doc(doc)
