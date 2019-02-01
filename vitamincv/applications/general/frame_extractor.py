from vitamincv.module_api.cvmodule import CVModule
from vitamincv.comm_apis.work_handler import WorkerManager

from vitamincv.applications.gen

from PIL import Image
from io import BytesIO
from imohash import hashfileobject
import boto3

class FrameExtractor(CVModule):
    def __init__(self, server_name, version):
        super().__init__(server_name, version)

        self._s3_bucket = "unified-frame-extraction"
        self._sql_db = ""

        self._s3_client = boto3.client("s3")

        self._encoding = "JPG"

        self._manager = WorkerManager(func=self._upload_frame_helper,
                                      n=10,
                                      max_queue_size=100,
                                      parallelization="thread")

    def _upload_frame_helper(self, data):
        self._upload_frame(**data)

    def _upload_frame(self, frame, tstamp, video_hash):
        im = Image.fromarray(frame)
        im_filelike = BytesIO()
        im.save(im_filelike, format=self._encoding)
        s3_key = "{}/{}.{}".format(video_hash, tstamp, self._encoding)
        result = self._s3_client.upload_fileobj(im_filelike,
                                                self._s3_bucket,
                                                s3_key)
        self._append_to_sql()

    def _append_to_sql(self):
        pass

    def process(self, message):
        self.set_message(message)
        video_url = self.avro_api.get_url()
        self.media_api = MediaRetriever(video_url)
        filelike = self.media_api.download(return_filelike=True)
        video_hash = hashfileobject(filelike, hexdigest=True)
        for frame, tstamp in self.media_api.get_frames_iterator():
            self._upload_frame(frame, tstamp, video_hash)
            # data = {
            #     "frame": frame,
            #     "tstamp": tstamp,
            #     "video_hash": video_hash
            # }
            # self._manager.queue.put(data)
