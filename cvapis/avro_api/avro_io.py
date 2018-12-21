import os
import sys
import glog as log
import json
import pkg_resources
import tempfile
import struct
import base64

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer, ContextStringIO, MAGIC_BYTE
from confluent_kafka.avro.serializer import SerializerError

from cvapis.avro_api import config

class AvroIO():
    def __init__(self, use_schema_registry=False, use_base64=True):
        """Public interface for Avro IO functionality
        
        Args:
            use_schema_registry (bool): flag to use schema registry via client ID and registry URL
            use_base64 (bool): encoding binary to base64
        """
        self.impl = None
        self.use_base64 = use_base64
        if use_schema_registry:
            self.impl = _AvroIORegistry()
            log.warning("Setting use_base64=False because use_schema_registry=True")
            self.use_base64 = False
        else:
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
        return str(self.impl.schema).replace("\\","")
    
    def decode_file(self, file_path):
        """Decode an Avro Binary using the CV schema

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

    def decode(self, bytes, binary_flag=True):
        """Decode an Avro Binary using the CV schema from bytes"""
        if self.use_base64:
            bytes_aux = base64.b64decode(bytes)
        if binary_flag:
            return self.impl.decode(bytes_aux)
        else:
            #log.info(str(bytes_aux))
            return json.loads(str(bytes_aux))
    
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

    def encode(self, doc):
        """Encode an avro doc to bytes"""
        bytes = self.impl.encode(doc)
        if self.use_base64:
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
    
    @staticmethod
    def read_json(file_path):
        """Convenience method for reading jsons"""
        return json.load(open(file_path))
    
    @staticmethod
    def write_json(json_str, file_path, indent=None):
        """Convenience method for writing jsons"""
        with open(file_path, "w") as wf:
            if type(json_str) is dict:
                json.dump(json_str, wf, indent=indent)
            elif type(json_str) is str:
                wf.write(json_str)
            else:
                raise ValueError("json_str input is not a str or dict. Of type: {}".format(type(json_str)))

##################################
# Private implementation classes #
##################################

class _AvroIOLocal():
    def __init__(self):
        """Private implementation class for Avro IO of local files"""
        local_schema_file = pkg_resources.resource_filename('cvapis.avro_api', 'image-science-response.avsc')
        log.debug("Using local schema file {}".format(local_schema_file))
        if not os.path.exists(local_schema_file):
            raise FileNotFoundError("Schema file not found")
        self.schema = avro.schema.Parse(open(local_schema_file).read())

    def decode(self, bytes):    
        if len(bytes) <= 5:
            raise SerializerError("Message is too small to decode")
        with ContextStringIO(bytes) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
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
            outf.write(struct.pack('b', MAGIC_BYTE))
            outf.write(struct.pack('>I', config.SCHEMA_ID))
            encoder = avro.io.BinaryEncoder(outf)
            writer = avro.io.DatumWriter(self.schema)
            writer.write(record, encoder)
            return outf.getvalue()

class _AvroIORegistry():
    def __init__(self):
        """Private implementation class for Avro IO using the registry"""
        log.info("Using registry with schema_id {}".format(config.SCHEMA_ID))
        try:
            self.client = CachedSchemaRegistryClient(url=config.REGISTRY_URL)
            self.schema = self.client.get_by_id(config.SCHEMA_ID)
            self.serializer = MessageSerializer(self.client)
        except:
            raise ValueError("Client id or schema id not found")

    def decode(self, bytes):
        return self.serializer.decode_message(bytes)

    def encode(self, record):
        return self.serializer.encode_record_with_schema_id(config.SCHEMA_ID, record)



 
