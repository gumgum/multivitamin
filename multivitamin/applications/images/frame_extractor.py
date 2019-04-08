import os
import json
from io import BytesIO

import boto3
import glog as log
import numpy as np
from PIL import Image

from multivitamin.module import PropertiesModule
from multivitamin.utils.work_handler import WorkerManager
from multivitamin.media import MediaRetriever
from multivitamin.data.response.utils import get_current_time
from multivitamin.data.response.dtypes import (
    Footprint,
    VideoAnn,
    Property,
)


def get_contents_file_s3_key(url, sample_rate=None):
    contents_file_name = "contents"
    contents_file_ext = "json"
    rel_path_format = "{video_id}/{filename}.{ext}"
    video_id = os.path.basename(url).rsplit(".", 1)[0]
    video_id = "".join([e for e in video_id if e.isalnum() or e in ["/", "."]])
    if video_id is not None:
        video_id += ".{}fps".format(sample_rate)
    contents_file_key = rel_path_format.format(
        video_id=video_id, filename=contents_file_name, ext=contents_file_ext
    )
    contents_file_key = "".join(
        [e for e in contents_file_key if e.isalnum() or e in ["/", "."]]
    )
    return contents_file_key


class FrameExtractor(PropertiesModule):
    def __init__(
        self,
        server_name,
        version,
        sample_rate=1.0,
        s3_bucket=None,
        local_dir=None,
        module_id_map=None,
        n_threads=100,
    ):
        super().__init__(server_name, version, module_id_map=module_id_map)

        self.n_threads = n_threads
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
            full_path = "".join(
                [e for e in full_path if e.isalnum() or e in ["/", "."]]
            )
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

    def process_properties(self):
        self._s3_write_manager = WorkerManager(
            func=self._upload_frame_helper,
            n=self.n_threads,
            max_queue_size=100,
            parallelization="thread",
        )

        self._local_write_manager = WorkerManager(
            func=self._write_frame_helper,
            n=self.n_threads,
            max_queue_size=100,
            parallelization="thread",
        )

        self.last_tstamp = 0.0
        log.info("Processing")
        # filelike = self.media_api.download(return_filelike=True)

        # if filelike.getbuffer().nbytes == 0:
        #     self.code = "ERROR_NO_IMAGES_LOADED"

        # log.info('Getting hash')
        # video_hash = hashfileobject(filelike, hexdigest=True)
        self.video_url = self.response.request.url
        self.med_ret = MediaRetriever(self.video_url)
        self.contents_file_key = get_contents_file_s3_key(self.video_url,
                                                          self._sample_rate)
        video_id = self.contents_file_key.split("/")[0]
        if self._local_dir is not None:
            self._mklocaldirs("{}/{}".format(self._local_dir, video_id))
            self._mklocaldirs("{}/{}/frames".format(self._local_dir, video_id))
            if os.path.exists("{}/{}".format(self._local_dir, self.contents_file_key)):
                log.info("Local Video already exists")

        try:
            self._s3_client.head_object(
                Bucket=self._s3_bucket, Key=self.contents_file_key
            )
            new_url = self._s3_url_format.format(
                bucket=self._s3_bucket, s3_key=self.contents_file_key
            )
            log.info("Video already exists")
            p = Property(
                server=self.name,
                ver=self.version,
                value=new_url,
                property_type="extraction",
                property_id=1,
            )
            track = VideoAnn(t1=0.0, t2=float(self.last_tstamp), props=[p])
            self.response.append_track(track)
            self._s3_write_manager.kill_workers_on_completion()
            self._local_write_manager.kill_workers_on_completion()
            return
        except:
            pass

        contents = []
        log.info("Getting frames")
        for i, (frame, tstamp_secs) in enumerate(
            self.med_ret.get_frames_iterator(sample_rate=self._sample_rate)
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

        self.response.url_original = self.video_url
        new_url = self._s3_url_format.format(
            bucket=self._s3_bucket, s3_key=self.contents_file_key
        )
        self.response.url = new_url
        p = Property(
            server=self.name,
            ver=self.version,
            value=new_url,
            property_type="extraction",
            property_id=1,
        )
        track = VideoAnn(t1=0.0, t2=float(self.last_tstamp), props=[p])
        self.response.append_track(track)
        self._s3_write_manager.kill_workers_on_completion()
        self._local_write_manager.kill_workers_on_completion()
