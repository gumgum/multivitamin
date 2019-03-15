import os
import json
import glog as log
import tempfile
import shutil
import boto3

from multivitamin.apis.comm_api import CommAPI
from multivitamin.data import Response


INDENTATION = 2


class S3API(CommAPI):
    def __init__(self, s3_bucket, s3_key):
        """ S3API is a CommAPI object that pushes responses to an
            s3_bucket/s3_key or pushing_folder

        Args:
            s3_bucket (str): s3 bucket
            s3_key (str): key of s3 bucket to write to 
        """
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def pull(self, n=1):
        raise NotImplementedError("S3API.pull() not yet implemented")

    def push(self, responses):
        """Push a list of Response objects to be written to pushing_folder

        Args:
            responses (list[Response]): list of requests to be pushed/written to disk
        """
        if not isinstance(responses, list):
            responses = [responses]

        for res in responses:
            assert(isinstance(res, Response))
            fn = self.get_fn(res)
            tmp_dir = tempfile.mkdtemp()
            outfn = os.path.join(tmp_dir, fn)

            log.info("Writing response to {}".format(outfn))
            if not os.path.exists(os.path.dirname(outfn)):
                os.makedirs(os.path.dirname(outfn))

            if res.request.bin_encoding is True:
                with open(outfn, "wb") as wf:
                    wf.write(res.bytes)
            else:
                with open(outfn, "w") as wf:
                    wf.write(json.dumps(res.dict, indent=INDENTATION))

            s3client = boto3.client("s3")
            key_fullpath = os.path.join(self.s3_key, fn)
            log.info("Pushing {} to {}/{}".format(outfn, self.s3_bucket, key_fullpath))
            with open(outfn, "rb") as data:
                s3client.put_object(Bucket=self.s3_bucket, Key=key_fullpath, Body=data)

            if os.path.exists(tmp_dir):
                log.info("Removing temp dir {}".format(tmp_dir))
                shutil.rmtree(tmp_dir)

    def get_fn(self, response):
        """Create a fn from url string from avro_api

        Args:
            aapi (avro_api): avro_api of response
        
        Returns:
            str: unique identifier
        """
        media_url = response.url
        ext = ".json"
        if response.request.bin_encoding is True:
            ext = ".avro"
        return os.path.basename(media_url) + ext
