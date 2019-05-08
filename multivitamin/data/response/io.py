import os
import sys
import glog as log
import json
import pkg_resources
import tempfile
import struct
import base64
import traceback

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import (
    MessageSerializer,
    ContextStringIO,
    MAGIC_BYTE,
)
from confluent_kafka.avro.serializer import SerializerError

from multivitamin.data.response import config


class AvroIO:
    def __init__(self, schema_registry_url=None):
        """Public interface for Avro IO functionality
        
        Args:
            use_schema_registry (bool): flag to use schema registry via client ID and registry URL
            use_base64 (bool): encoding binary to base64
        """
        self.impl = None
        self.use_base64 = False
        if schema_registry_url:
            log.info(f"schema_registry_url: {schema_registry_url}")
            self.impl = _AvroIORegistry(schema_registry_url)
        else:
            log.warning("registry_url is None, using local schema and serializing w/o magic byte")
            self.impl = _AvroIOLocal()

    def get_schema(self):
        """Return schema 

        Returns:
            avro.schema.RecordSchema: schema
        """
        return self.impl.schema

    def get_schema_str(self):
        """Return schema as str

        Returns:
            str: schema as str
        """
        return str(self.impl.schema).replace("\\", "")

    def decode_binary_file(self, file_path):
        """Decode an Avro Binary using the schema

        Args:
            file_path (str) : avro binary file

        Returns:
            dict: avro document
        """
        if not os.path.exists(file_path):
            log.error("Missing: {}".format(file_path))
            raise FileNotFoundError("Missing: {}".format(file_path))
        log.info("Decoding file: {}".format(file_path))
        return self.decode(open(file_path, "rb").read())

    def decode(self, bytes, use_base64=False, binary_flag=True):
        """Decode an Avro Binary using the CV schema from bytes
        
        Args:
            bytes
            use_base64
            binary_flag
        
        Returns:
            dict:
        """
        if use_base64:
            bytes = base64.b64decode(bytes)
        if binary_flag:
            return self.impl.decode(bytes)
        else:
            return json.loads(str(bytes))

    def write(self, doc, file, serialize=True, indent=None):
        """Write Avro doc 

        Args:
            doc (dict): dict of the avro document
            file_path (str): avro binary file output
            serialize (bool): whether to serialize avro doc to a binary file
            indent (int): if serialize=False, write json with indentation=indent
        
        Returns:
            bool: True if successfully wrote file
        """
        if serialize:
            try:
                bytes = self.encode(doc)
                with open(file, "wb") as wf:
                    wf.write(bytes)
            except avro.io.AvroTypeException:
                log.error("avro.io.AvroTypeException: the datum is not an example of the schema")
                return False
            log.info("Encoded doc to file: {}".format(file))
        else:
            if not self.is_valid_avro_doc(doc):
                log.error("datum is not an example of schema")
                return False
            with open(file, "w") as wf:
                json.dump(doc, wf, indent=indent)
        return True

    def encode(self, doc, use_base64=False):
        """Encode an avro doc to bytes
        """
        bytes = self.impl.encode(doc)
        if use_base64:
            log.info(f"use_base64={use_base64}")
            bytes = base64.b64encode(bytes)
        return bytes

    def is_valid_avro_doc(self, doc):
        """Boolean test to validate json against a schema

        Args:
            doc (dict): avro doc as a dict

        Returns:
            boolean: True if json is an example of schema
        """
        try:
            writer = DataFileWriter(tempfile.TemporaryFile(), DatumWriter(), self.impl.schema)
            writer.append(doc)
            writer.close()
        except:
            return False
        return True

    @staticmethod
    def is_valid_avro_doc_static(doc, schema):
        """Boolean test to validate json against a schema

        Args:
            doc (dict): avro doc as a dict
            schema (str or dict): schema as a string or dict

        Returns:
            boolean: True if json is an example of schema
        """
        if isinstance(schema, str):
            avro_schema = avro.schema.Parse(schema)
        else:
            avro_schema = schema
        try:
            writer = DataFileWriter(tempfile.TemporaryFile(), DatumWriter(), avro_schema)
            writer.append(doc)
            writer.close()
        except:
            return False
        return True


class _AvroIOLocal:
    def __init__(self):
        """Private implementation class for Avro IO of local files"""
        local_schema_file = pkg_resources.resource_filename(
            "multivitamin.data.response", config.SCHEMA_FILE
        )
        log.debug("Using local schema file {}".format(local_schema_file))
        if not os.path.exists(local_schema_file):
            raise FileNotFoundError("Schema file not found")
        self.schema = avro.schema.Parse(open(local_schema_file).read())

    def decode(self, bytes):
        if len(bytes) <= 5:
            raise SerializerError("Message is too small to decode")
        with ContextStringIO(bytes) as payload:
            magic, schema_id = struct.unpack(">bI", payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")
            curr_pos = payload.tell()
            avro_reader = avro.io.DatumReader(self.schema)

            def decoder(p):
                bin_decoder = avro.io.BinaryDecoder(p)
                return avro_reader.read(bin_decoder)

            return decoder(payload)

    def encode(self, record):
        with ContextStringIO() as outf:
            outf.write(struct.pack("b", MAGIC_BYTE))
            outf.write(struct.pack(">I", config.SCHEMA_ID))
            encoder = avro.io.BinaryEncoder(outf)
            writer = avro.io.DatumWriter(self.schema)
            writer.write(record, encoder)
            return outf.getvalue()


class _AvroIORegistry:
    def __init__(self, schema_registry_url):
        """Private implementation class for Avro IO using the registry"""
        log.info(f"Using registry with schema_url/id {schema_registry_url}/{config.SCHEMA_ID}")
        try:
            self.client = CachedSchemaRegistryClient(url=schema_registry_url)
            self.schema = self.client.get_by_id(config.SCHEMA_ID)
            self.serializer = MessageSerializer(self.client)
        except:
            raise ValueError("Client id or schema id not found")

    def decode(self, bytes):
        return self.serializer.decode_message(bytes)

    def encode(self, record):
        return self.serializer.encode_record_with_schema_id(config.SCHEMA_ID, record)
