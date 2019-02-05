from vitamincv.module_api.cvmodule import CVModule
from vitamincv.comm_apis.work_handler import WorkerManager
from vitamincv.media_api.media import MediaRetriever
from vitamincv.avro_api.utils import get_current_time
from vitamincv.avro_api.cv_schema_factory import *

from PIL import Image
from io import BytesIO
# from imohash import hashfileobject
import boto3

import os

class FrameExtractor(CVModule):
    def __init__(self, server_name, version, sample_rate=1.0, s3_bucket=None, local_dir=None):
        super().__init__(server_name, version)
        self._sample_rate = sample_rate

        self._local_dir = local_dir
        self._s3_bucket = s3_bucket
        self._list_file = "contents"
        self._rel_path_format = "{video_hash}/{filename}.{ext}"
        self._s3_url_format = "https://s3.amazonaws.com/{bucket}/{s3_key}"

        self._sql_db = ""

        self._s3_client = boto3.client("s3")

        self._encoding = "JPEG"

        self._s3_write_manager = WorkerManager(func=self._upload_frame_helper,
                                      n=100,
                                      max_queue_size=100,
                                      parallelization="thread")

        self._local_write_manager = WorkerManager(func=self._write_frame_helper,
                                      n=100,
                                      max_queue_size=100,
                                      parallelization="thread")

    @staticmethod
    def _mklocaldirs(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _write_frame_helper(self, data):
        self._write_frame(**data)

    def _write_frame(self, frame, tstamp, video_hash):
        relative_path = self._rel_path_format.format(video_hash=video_hash, filename=tstamp, ext=self._encoding)
        full_path = "{}/{}".format(self._local_dir, relative_path)
        im_filelike = self._convert_frame_to_filelike(frame)
        with open(full_path, "wb") as f:
            f.write(im_filelike.read())

    def _add_contents_to_local(self, contents):
        filelike = BytesIO()
        for video_hash, tstamp in contents:
            relative_path = self._rel_path_format.format(video_hash=video_hash, filename=tstamp, ext=self._encoding)
            full_path = "{}/{}".format(self._local_dir, relative_path)
            line = "{}\t{}\n".format(tstamp, full_path)
            filelike.write(line.encode())
        filelike.seek(0)

        with open("{}/{}".format(self._local_dir, self.contents_file_key), "wb") as f:
            f.write(filelike.read())

    def _convert_frame_to_filelike(self, frame):
        im = Image.fromarray(frame)
        im_filelike = BytesIO()
        im.save(im_filelike, format=self._encoding)
        im_filelike.seek(0)
        return im_filelike

    def _upload_frame_helper(self, data):
        self._upload_frame(**data)

    def _upload_frame(self, frame, tstamp, video_hash):
        im_filelike = self._convert_frame_to_filelike(frame)
        s3_key = self._rel_path_format.format(video_hash=video_hash, filename=tstamp, ext=self._encoding)
        result = self._s3_client.upload_fileobj(im_filelike,
                                                self._s3_bucket,
                                                s3_key)
        self._append_to_sql(video_hash, tstamp, s3_key, result)

    def _append_to_sql(self, video_hash, tstamp, s3_key, result):
        pass

    def _add_contents_to_s3(self, contents):
        filelike = BytesIO()
        for video_hash, tstamp in contents:
            s3_key = self._rel_path_format.format(video_hash=video_hash, filename=tstamp, ext=self._encoding)
            im_url = self._s3_url_format.format(bucket=self._s3_bucket, s3_key=s3_key)
            line = "{}\t{}\n".format(tstamp, im_url)
            filelike.write(line.encode())
        filelike.seek(0)

        result = self._s3_client.upload_fileobj(filelike,
                                                self._s3_bucket,
                                                self.contents_file_key)
        return result

    def process(self, message):
        self.set_message(message)
        self.code = "SUCCESS"
        self.last_tstamp = 0.0
        log.info('Processing')
        # filelike = self.media_api.download(return_filelike=True)

        # if filelike.getbuffer().nbytes == 0:
        #     self.code = "ERROR_NO_IMAGES_LOADED"

        #log.info('Getting hash')
        # video_hash = hashfileobject(filelike, hexdigest=True)
        video_hash = os.path.basename(self.request_api.request["url"]).rsplit(".", 1)[0]
        self.contents_file_key = self._rel_path_format.format(video_hash=video_hash, filename=self._list_file, ext="tsv")

        if self._local_dir is not None:
            self._mklocaldirs("{}/{}".format(self._local_dir, video_hash))
            if os.path.exists("{}/{}".format(self._local_dir, self.contents_file_key)):
                log.info("Local Video already exists")

        try:
            self._s3_client.head_object(Bucket=self._s3_bucket,
                                    Key=self._rel_path_format.format(video_hash=video_hash,
                                                                   filename=self._list_file,
                                                                   ext="tsv"))
            log.info("Video already exists")
            return
        except:
            pass

        contents = []
        log.info('Getting frames')
        for i,(frame, tstamp) in enumerate(self.media_api.get_frames_iterator(sample_rate=self._sample_rate)):
            #self._upload_frame(frame, tstamp, video_hash)
            if i%100==0:
              log.info('...tstamp: ' + str(tstamp))
            log.debug('tstamp: ' + str(tstamp))
            if frame is None:
                continue
            self.last_tstamp = tstamp
            data = {
                "frame": frame,
                "tstamp": tstamp,
                "video_hash": video_hash
            }
            contents.append((video_hash, tstamp))

            if self._local_dir is not None:
                self._local_write_manager.queue.put(data)

            if self._s3_bucket is not None:
                self._s3_write_manager.queue.put(data)

        # self._s3_write_manager.kill_workers_on_completion()
        # self._local_write_manager.kill_workers_on_completion()
        if self._s3_bucket is not None:
            result = self._add_contents_to_s3(contents)
        if self._local_dir is not None:
            self._add_contents_to_local(contents)

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
        url = self._s3_url_format.format(bucket=self._s3_bucket, s3_key=self.contents_file_key)
        p = create_prop(server=self.name, value=url, property_type="extraction")
        track = create_video_ann(t1=0.0, t2=self.last_tstamp, props=[p])
        self.avro_api.append_track_to_tracks_summary(track)
