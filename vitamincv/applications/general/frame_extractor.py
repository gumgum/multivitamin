from vitamincv.module_api.cvmodule import CVModule
from vitamincv.comm_apis.work_handler import WorkerManager
from vitamincv.media_api.media import MediaRetriever
from vitamincv.avro_api.utils import get_current_time
from vitamincv.avro_api.cv_schema_factory import *

from PIL import Image
from io import BytesIO
from imohash import hashfileobject
import boto3

class FrameExtractor(CVModule):
    def __init__(self, server_name, version):
        super().__init__(server_name, version)

        self._s3_bucket = "unified-frame-extraction"
        self._list_file = "contents"
        self._s3_key_format = "{video_hash}/{filename}.{ext}"
        self._s3_url_format = "https://s3.amazonaws.com/{bucket}/{s3_key}"

        self._sql_db = ""

        self._s3_client = boto3.client("s3")

        self._encoding = "JPEG"

        self._manager = WorkerManager(func=self._upload_frame_helper,
                                      n=100,
                                      max_queue_size=100,
                                      parallelization="thread")

    def _upload_frame_helper(self, data):
        self._upload_frame(**data)

    def _upload_frame(self, frame, tstamp, video_hash):
        im = Image.fromarray(frame)
        im_filelike = BytesIO()
        im.save(im_filelike, format=self._encoding)
        im_filelike.seek(0)
        s3_key = self._s3_key_format.format(video_hash=video_hash, filename=tstamp, ext=self._encoding)
        result = self._s3_client.upload_fileobj(im_filelike,
                                                self._s3_bucket,
                                                s3_key)

        self._append_to_sql(video_hash, tstamp, s3_key, result)

    def _append_to_sql(self, video_hash, tstamp, s3_key, result):
        pass

    def _add_contents_to_s3(self, contents):
        filelike = BytesIO()
        for video_hash, tstamp in contents:
            s3_key = self._s3_key_format.format(video_hash=video_hash, filename=tstamp, ext=self._encoding)
            im_url = self._s3_url_format.format(bucket=self._s3_bucket, s3_key=s3_key)
            line = "{}\t{}\n".format(tstamp, im_url)
            filelike.write(line.encode())
        filelike.seek(0)

        contents_file_key = self._s3_key_format.format(video_hash=video_hash, filename=self._list_file, ext="tsv")
        result = self._s3_client.upload_fileobj(filelike,
                                                self._s3_bucket,
                                                s3_key)
        return result

    def process(self, message):
        self.set_message(message)
        self.code = "SUCCESS"
        self.last_tstamp = 0.0

        filelike = self.media_api.download(return_filelike=True)

        if filelike.getbuffer().nbytes == 0:
            self.code = "ERROR_NO_IMAGES_LOADED"

        video_hash = hashfileobject(filelike, hexdigest=True)

        contents = []
        for frame, tstamp in self.media_api.get_frames_iterator(sample_rate=1.0):
            #self._upload_frame(frame, tstamp, video_hash)
            if frame is None:
                continue
            self.last_tstamp = tstamp
            data = {
                "frame": frame,
                "tstamp": tstamp,
                "video_hash": video_hash
            }
            contents.append((video_hash, tstamp))
            self._manager.queue.put(data)
        self._manager.kill_workers_on_completion()
        result = self._add_contents_to_s3(contents)
        print(result)

    def update_response(self):
        date = get_current_time()
        n_footprints=len(self.avro_api.get_footprints())
        footprint_id=date+str(n_footprints+1)
        fp=create_footprint(code=self.code, ver=self.version, company="gumgum", labels=None, server_track="",
                     server=self.name, date=date, annotator="",
                     tstamps=None, id=footprint_id)
        self.avro_api.append_footprint(fp)

        if self.code != "SUCCESS":
            log.error('The processing was not succesful')
            return
        self.avro_api.set_url(self.request_api.get_url())
        self.avro_api.set_url_original(self.request_api.get_url())
        self.avro_api.set_dims(*self.request_api.media_api.get_w_h())
        p = create_prop(server=self.name, value=self.s3_contents_file, property_type="extraction")
        track = create_video_ann(t1=0.0, t2=self.last_tstamp, props=[p])
        self.avro_api.append_track_to_tracks_summary(track)
