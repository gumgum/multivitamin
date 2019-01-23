import os
import sys
import glog as log
import tempfile
import shutil
import boto3

from vitamincv.comm_apis.comm_api import CommAPI
from vitamincv.avro_api.utils import get_current_date
from vitamincv.avro_api.avro_io import AvroIO
INDENTATION=2

class DumpResponse(CommAPI):
    def __init__(self, pushing_folder=None, s3_bucket=None, s3_key=None):
        """DumpResponse is a CommAPI object that converts responses to csv and then either pushes 
        to s3_bucket/s3_key or pushing_folder

        Args:
            pushing_folder (str): where to write responses if local
            s3_bucket (str): s3 bucket
            s3_key (str): key of s3 bucket to write to 
        """
        self.pushing_folder = pushing_folder
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        if pushing_folder:
            if not os.path.exists(pushing_folder):
                os.makedirs(pushing_folder)        
        if s3_bucket and not s3_key:
            raise ValueError("s3 bucket defined but s3 key not defined")
        if s3_key and not s3_bucket:
            raise ValueError("s3 key defined but s3 bucket not defined")
        if not pushing_folder and not s3_key:
            raise ValueError("pushing_folder and s3 key not defined, we cannot set where to dump.")

    def pull(self, n=1):
        raise ValueError("Response2s3 cannot be used as a pulling CommAPI, only pushing")

    def push(self, request_apis):
        """Push a list of RequestAPI objects to be written to pushing_folder

        Args:
            request_apis (list[RequestAPI]): list of requests to be pushed/written to disk
        """
        if not isinstance(request_apis, list):
            request_apis = [request_apis]

        for req in request_apis:
            response = req.get_response(indent=INDENTATION)
            
            outfn = None
            fname_local = self.get_fname(req.avro_api)
            if self.pushing_folder:
                outfn = os.path.join(self.pushing_folder, get_current_date(), fname_local)
            else:
                tmp_dir = tempfile.mkdtemp()
                outfn = os.path.join(tmp_dir, fname_local)

            log.info("Writing response to {}".format(outfn))
            if not os.path.exists(os.path.dirname(outfn)):
                os.makedirs(os.path.dirname(outfn))

            AvroIO.write_json(req.avro_api.get_json(indent=INDENTATION), outfn, indent=INDENTATION)

            if self.s3_bucket and self.s3_key:
                s3client = boto3.client('s3')
                key_fullpath =os.path.join(self.s3_key,fname_local)
                log.info("Pushing {} to {}/{}".format(outfn, self.s3_bucket, key_fullpath))
                with open(outfn, 'rb') as data:
                    s3client.put_object(Bucket=self.s3_bucket, Key=key_fullpath, Body=data)

            if not self.pushing_folder:
                if os.path.exists(tmp_dir):
                    log.info("Removing temp dir {}".format(tmp_dir))
                    shutil.rmtree(tmp_dir)


    def get_fname(self, aapi):
        """Create a fn from url string from avro_api

        Args:
            aapi (avro_api): avro_api of response
        
        Returns:
            str: unique identifier
        """
        media_url = aapi.get_url()
        return os.path.splitext(os.path.basename(media_url))[0] + ".json"