from multivitamin.module_api.cvmodule import CVModule
from multivitamin.utils.work_handler import WorkerManager
from multivitamin.media_api.media import MediaRetriever
from multivitamin.avro_api.utils import get_current_time
from multivitamin.avro_api.cv_schema_factory import *

from PIL import Image
from io import BytesIO
import numpy as np
import boto3
import json

import os


class FrameExtractor(CVModule):
    def __init__(
        self,
        server_name,
        version,
        sample_rate=1.0,
        s3_bucket=None,
        local_dir=None,
        module_id_map=None,
    ):
        super().__init__(server_name, version, module_id_map=module_id_map)
        self._sample_rate = sample_rate

        self._local_dir = local_dir
        self._s3_bucket = s3_bucket
        self._list_file = "contents"
        self._img_name_format = "{tstamp:010d}"
        self._rel_path_format = "{video_id}/{filename}.{ext}"
        self._image_rel_path_format = "{video_id}/frames/{filename}.{ext}"
        self._s3_url_format = "https://s3.amazonaws.com/{bucket}/{s3_key}"

        self._sql_db = ""

        self._s3_client = boto3.client("s3")

        self._encoding = "JPEG"
        self._content_type = "image/jpeg"
        self._s3_upload_args = {"ContentType": self._content_type}

        self._s3_write_manager = WorkerManager(
            func=self._upload_frame_helper, n=100, max_queue_size=100, parallelization="thread"
        )

        self._local_write_manager = WorkerManager(
            func=self._write_frame_helper, n=100, max_queue_size=100, parallelization="thread"
        )

    @staticmethod
    def _mklocaldirs(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _write_frame_helper(self, data):
        try:
            self._write_frame(**data)
        except:
            log.error("Local Write Failed")

    def _write_frame(self, frame, tstamp, video_id):
        filename = self._img_name_format.format(tstamp=tstamp)
        relative_path = self._image_rel_path_format.format(
            video_id=video_id, filename=filename, ext=self._encoding
        )
        full_path = "{}/{}".format(self._local_dir, relative_path)
        full_path = "".join([e for e in full_path if e.isalnum() or e in ["/", "."]])
        im_filelike = self._convert_frame_to_filelike(frame)
        with open(full_path, "wb") as f:
            f.write(im_filelike.read())

    def _add_contents_to_local(self, contents):
        filelike = BytesIO()
        contents_json = {"original_url": self.video_url, "frames": []}
        for video_id, tstamp in contents:
            filename = self._img_name_format.format(tstamp=tstamp)
            relative_path = self._image_rel_path_format.format(
                video_id=video_id, filename=filename, ext=self._encoding
            )
            full_path = "{}/{}".format(self._local_dir, relative_path)
            full_path = "".join([e for e in full_path if e.isalnum() or e in ["/", "."]])
            # line = "{}\t{}\n".format(tstamp, full_path)
            # filelike.write(line.encode())
            contents_json["frames"].append((tstamp, full_path))
        filelike.write(json.dumps(contents_json, indent=2).encode())
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
        try:
            self._upload_frame(**data)
        except:
            log.error("Failed to write Frame to S3")

    def _upload_frame(self, frame, tstamp, video_id):
        filename = self._img_name_format.format(tstamp=tstamp)
        im_filelike = self._convert_frame_to_filelike(frame)
        s3_key = self._image_rel_path_format.format(
            video_id=video_id, filename=filename, ext=self._encoding
        )
        s3_key = "".join([e for e in s3_key if e.isalnum() or e in ["/", "."]])
        result = self._s3_client.upload_fileobj(
            im_filelike, self._s3_bucket, s3_key, ExtraArgs=self._s3_upload_args
        )
        self._append_to_sql(video_id, tstamp, s3_key, result)

    def _append_to_sql(self, video_id, tstamp, s3_key, result):
        pass

    def _add_contents_to_s3(self, contents):
        filelike = BytesIO()
        contents_json = {"original_url": self.video_url, "frames": []}
        for video_id, tstamp in contents:
            filename = self._img_name_format.format(tstamp=tstamp)
            s3_key = self._image_rel_path_format.format(
                video_id=video_id, filename=filename, ext=self._encoding
            )
            s3_key = "".join([e for e in s3_key if e.isalnum() or e in ["/", "."]])
            im_url = self._s3_url_format.format(bucket=self._s3_bucket, s3_key=s3_key)
            # line = "{}\t{}\n".format(tstamp, im_url)
            # filelike.write(line.encode())
            contents_json["frames"].append((tstamp, im_url))
        filelike.write(json.dumps(contents_json, indent=2).encode())
        filelike.seek(0)

        result = self._s3_client.upload_fileobj(
            filelike,
            self._s3_bucket,
            self.contents_file_key,
            ExtraArgs={"ContentType": "application/json"},
        )
        return result

    def process(self, message):
        self.set_message(message)
        self.code = "SUCCESS"
        self.last_tstamp = 0.0
        log.info("Processing")
        # filelike = self.media_api.download(return_filelike=True)

        # if filelike.getbuffer().nbytes == 0:
        #     self.code = "ERROR_NO_IMAGES_LOADED"

        # log.info('Getting hash')
        # video_hash = hashfileobject(filelike, hexdigest=True)
        self.video_url = self.request_api.request["url"]
        video_id = os.path.basename(self.video_url).rsplit(".", 1)[0]
        video_id = "".join([e for e in video_id if e.isalnum() or e in ["/", "."]])
        self.contents_file_key = self._rel_path_format.format(
            video_id=video_id, filename=self._list_file, ext="json"
        )
        self.contents_file_key = "".join(
            [e for e in self.contents_file_key if e.isalnum() or e in ["/", "."]]
        )
        if self._local_dir is not None:
            self._mklocaldirs("{}/{}".format(self._local_dir, video_id))
            self._mklocaldirs("{}/{}/frames".format(self._local_dir, video_id))
            if os.path.exists("{}/{}".format(self._local_dir, self.contents_file_key)):
                log.info("Local Video already exists")

        try:
            self._s3_client.head_object(Bucket=self._s3_bucket, Key=self.contents_file_key)
            log.info("Video already exists")
            return
        except:
            pass

        contents = []
        log.info("Getting frames")
        for i, (frame, tstamp_secs) in enumerate(
            self.media_api.get_frames_iterator(sample_rate=self._sample_rate)
        ):
            tstamp = int(tstamp_secs * 1000)
            # self._upload_frame(frame, tstamp, video_hash)
            if i % 100 == 0:
                log.info("...tstamp: " + str(tstamp))
            log.debug("tstamp: " + str(tstamp))
            if frame is None:
                continue
            frame = np.ascontiguousarray(frame[:, :, ::-1])  # RGB to BGR
            self.last_tstamp = tstamp
            data = {"frame": frame, "tstamp": tstamp, "video_id": video_id}
            contents.append((video_id, tstamp))

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
        n_footprints = len(self.avro_api.get_footprints())
        footprint_id = date + str(n_footprints + 1)
        fp = create_footprint(
            code=self.code,
            ver=self.version,
            company="gumgum",
            labels=None,
            server_track="",
            server=self.name,
            date=date,
            annotator="",
            tstamps=None,
            id=footprint_id,
        )
        self.avro_api.append_footprint(fp)

        if self.code != "SUCCESS":
            log.error("The processing was not succesful")
            return
        self.avro_api.set_url(self.request_api.get_url())
        self.avro_api.set_url_original(self.request_api.get_url())
        self.avro_api.set_dims(*self.request_api.media_api.get_w_h())
        url = self._s3_url_format.format(bucket=self._s3_bucket, s3_key=self.contents_file_key)
        module_id = 0
        if self.module_id_map:
            module_id = self.module_id_map.get(self.name, 0)
        p = create_prop(
            server=self.name,
            ver=self.version,
            value=url,
            property_type="extraction",
            footprint_id=fp["id"],
            property_id=1,
            module_id=module_id,
        )
        track = create_video_ann(t1=0.0, t2=self.last_tstamp, props=[p])
        self.avro_api.append_track_to_tracks_summary(track)
