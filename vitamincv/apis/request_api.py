import os
import json
import glog as log
import urllib
from vitamincv.avro_api.avro_api import AvroAPI
from vitamincv.avro_api.avro_io import AvroIO
from vitamincv.media_api.media import MediaRetriever

import boto3


class RequestAPI:
    def __init__(self, request, request_id="", load_media=True):
        self.prod_flag = os.getenv("PRODUCTION")
        if not self.prod_flag:
            log.warning("os.getenv('PRODUCTION') is not set. Defaulting to True")
            self.prod_flag = 1
        self.request = {}
        self.request_id = request_id
        self.avro_api = AvroAPI()  # Default value
        self.media_api = MediaRetriever()  # Default value
        self.sample_rate = 1.0
        self.bin_encoding = False
        self.bin_decoding = False
        if "sample_rate" in self.request:
            self.sample_rate = self.request["sample_rate"]
        try:
            if type(request) == type({}):  # dictionary
                log.info("The request is a dictionary.")
                self.request = (
                    request
                )  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            elif type(request) == type(""):  # string
                log.info("The request is a string.")
                if len(request) == 0:
                    log.warning("Empty request.")
                    return
                else:
                    # log.info("We get the json.")
                    request_aux = json.loads(request)
                    # log.info("Json loaded.")
                    if type(request_aux) == type([]):  # it's an array
                        log.info("The request is an array.")
                        if (
                            request_aux[0]
                            == "com.amazon.sqs.javamessaging.MessageS3Pointer"
                        ):
                            messageS3Pointer = request_aux[1]
                            # log.info("type(messageS3Pointer): " + str(type(messageS3Pointer)))
                            # log.info("messageS3Pointer: " + str(messageS3Pointer))
                            s3BucketName = messageS3Pointer["s3BucketName"]
                            s3Key = messageS3Pointer["s3Key"]
                            log.info("s3BucketName: " + str(s3BucketName))
                            log.info("s3Key: " + str(s3Key))
                            # log.info("Creating a s3 client.")
                            s3_client = boto3.client("s3")
                            try:
                                log.info("Retrieving s3 object.")
                                obj = s3_client.get_object(
                                    Bucket=s3BucketName, Key=s3Key
                                )
                                request_data = obj["Body"].read().decode("utf-8")
                                log.info("Data retrieved from s3.")
                                # log.info("request_data: " + str(request_data))
                                # log.info("type(request_data): " + str(type(request_data)))
                            except ValueError as e:
                                log.warning(e)
                                log.warning("Problems retrieving request from s3.")
                                return
                            # log.info("getting the dictionary from json.")
                            request_raw = json.loads(request_data)
                        log.info("Checking request url encoding.")
                        self.request = RequestAPI.format_request(
                            request_raw
                        )  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                        # self.request=request_raw
                        # We need here the above line, for sure, to re-encoding of the url
                        # We need a checking encoding funtion for making it happen in the cases we are not sure.
                    else:
                        # log.info("we got a single json.")
                        self.request = (
                            request_aux
                        )  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        except ValueError as e:
            log.warning(e)
            log.error("No valid request: " + str(request))
            return

        # log.info('self.request: ' + str(self.request))
        try:
            # if previous response in anyway, we properly create AvroAPI
            # we get whether we are talking here about bin or not bin decoding

            self.bin_encoding = self.request.get("bin_encoding") == "true"

            bin_decoding_flag = True  # CAREFUL HERE
            if "bin_decoding" in self.request:
                bin_decoding_flag = (
                    self.request["bin_decoding"] == "true"
                    or self.request["bin_decoding"] == "True"
                )
            if "prev_response_url" in self.request:
                if len(self.request["prev_response_url"]) > 0:
                    if bin_decoding_flag:
                        avro_io = AvroIO(
                            use_schema_registry=self.prod_flag, use_base64=True
                        )
                        self.avro_api = AvroAPI(
                            avro_io.decode_file(self.request["prev_response_url"])
                        )
                    else:
                        self.avro_api = AvroAPI(
                            AvroIO.read_json(self.request["prev_response_url"])
                        )
            elif "prev_response" in self.request:
                if len(self.request["prev_response"]) > 0:
                    # log.info("type(self.request['prev_response']): " +str(type(self.request['prev_response'])))
                    # log.info("self.request['prev_response']: " + self.request['prev_response'])
                    if bin_decoding_flag:
                        avro_io = AvroIO(
                            use_schema_registry=self.prod_flag, use_base64=True
                        )
                        log.info("Decoding binary.")
                        self.avro_api = AvroAPI(
                            avro_io.decode(self.request["prev_response"])
                        )
                    else:
                        avro_io = AvroIO(use_base64=False)
                        log.info("Decoding json.")
                        self.avro_api = AvroAPI(
                            avro_io.decode(self.request["prev_response"], False)
                        )
                else:
                    self.avro_api = AvroAPI()
        except Exception as e:
            log.warning(
                "Problems parsing previous response with exception: {}".format(e)
            )
            self.avro_api = AvroAPI()
        self.reset_media_api(load_media)

    def reset_media_api(self, load_media=True):
        url = ""
        if "url" in self.request and load_media:
            url = self.request["url"]
            log.info("media url:" + url)
            try:
                self.media_api = MediaRetriever(url)
            except:
                log.warning("Not able to retrieve the media with url " + url)
                self.media_api = MediaRetriever()
        else:
            if load_media:
                log.warning("No available url.")
            self.media_api = MediaRetriever()

    def get_response(self, indent=None):
        """Get response from Request API obj

        Args:
            indent (int): if bin_encoding is False, return json with indentation=indent
        
        Returns:
            str or bytes, depending on 'bin_encoding' flag
        """
        if self.bin_encoding:
            avro_io = AvroIO(use_schema_registry=self.prod_flag, use_base64=True)
            doc = self.avro_api.get_response()
            log.debug("doc: " + str(doc))
            log.debug("Encoding doc")
            try:
                bytes = avro_io.encode(doc)
            except ValueError as e:
                log.error(e)
            log.debug("len(bytes): " + str(len(bytes)))
            return bytes
        else:
            avro_io = AvroIO(use_base64=False)
            log.debug("bin_encoding is False, returning string")
            return json.dumps(self.avro_api.get_response(), indent=indent)

    def get_destination_url(self):
        dst_url = self.get("dst_url")
        dst_url = self.request.get("dst_url", None)
        if not dst_url:
            return None
        # request_id = self.request_id
        # prefix='&'
        # if dst_url.find('?')<0:
        # prefix='?'
        # return dst_url + prefix + 'requestId='+request_id
        return dst_url

    def get_request_id(self):
        return self.request_id

    def get_avro_api(self):
        return self.avro_api

    def get_media_api(self):
        return self.media_api

    def get(self, key, default=None):
        if key in self.request:
            return self.request[key]
        return default

    def get_url(self):
        return self.request.get("url")

    def is_in(self, key):
        return key in self.request

    def get_keys(self):
        return self.request.keys()

    @staticmethod
    def standardize_request(request):
        """
        Args:
        : request: dictionary containing image and destination url along with other info
        : return same request structure with image url and dst_url standardized
        """
        formatted_request = format_request(request)
        return formatted_request

    @staticmethod
    def format_request(request):
        """
        
        : return formatted request
        """
        log.info("Formatting urls in request")
        if "url" in request:
            urls = [request["url"]]
        if "dst_url" in request:
            if request["dst_url"] != None:
                urls.append(request["dst_url"])
        log.info("urls: " + str(urls))
        for index, url in enumerate(urls):
            url = url.replace("&amp;", "&")
            url = url.replace(" ", "\\ ")
            url = url.replace("https://", "http://")
            url = url.replace("s://", "http://")
            url = url.replace("s:", "http://")
            url = url.replace("https://", "")
            url = url.replace("http://s.yimg.com", "https://s.yimg.com")
            url = url.replace(" ", "%20")
            if index == 0:
                request["url"] = url
            if index == 1:
                request["dst_url"] = url
        return request
