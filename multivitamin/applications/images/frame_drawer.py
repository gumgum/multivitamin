import os
import shutil
import hashlib
import random
import traceback

import boto3
import glog as log
import cv2
from colour import Color

from multivitamin.module import PropertiesModule
from multivitamin.data import Response
from multivitamin.data.response.utils import p0p1_from_bbox_contour
from multivitamin.data.response.dtypes import Property, VideoAnn
from multivitamin.media import MediaRetriever
from tempfile import TemporaryDirectory, NamedTemporaryFile
from glob import iglob

COLORS = [
    'darkorchid', 'darkgreen', 'coral', 'darkseagreen',
    'forestgreen', 'firebrick', 'olivedrab', 'steelblue',
    'tomato', 'yellowgreen'
]


def random_bgr():
    color = Color(COLORS[random.randint(0, len(COLORS)-1)]).rgb[::-1]
    return [x*255 for x in color]


def get_props_from_region(region):
    prop_strs = []
    for prop in region["props"]:
        try:
           prop["value"] = "{:.2f}".format(float(prop["value"]))
        except:
            pass
        out = "{}_{:.2f}".format(prop["value"], prop["confidence"])
        prop_strs.append(out)
    return prop_strs


DEFAULT_DUMP_FOLDER = '/tmp'


class FrameDrawer(PropertiesModule):
    def __init__(
        self,
        response=None,
        med_ret=None,
        server_name='FrameDrawer',
        version='1.0.0',
        module_id_map=None,
        pushing_folder=DEFAULT_DUMP_FOLDER,
        s3_bucket=None,
        s3_key=None
    ):
        super().__init__(
            server_name=server_name,
            version=version,
            module_id_map=module_id_map
        )

        if isinstance(response, dict):
            response = Response(response)

        self.response = response

        if not isinstance(self.response, Response):
            log.debug("No response set")
            self.response = None

        self.med_ret = med_ret
        if med_ret is None and self.response is not None:
            self.med_ret = MediaRetriever(self.response.url)

        if s3_bucket and not s3_key:
            raise ValueError("s3 bucket defined but s3_key not defined")

        if s3_key and not s3_key:
            raise ValueError("s3_key defined but s3 bucket not defined")

        if not pushing_folder and not s3_key:
            raise ValueError("pushing_folder and s3_key not defined, "
                             "we cannot set where to dump.")

        self.local_dir = pushing_folder
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key

    def process_properties(self,
                           dump_video=True,
                           dump_images=False,
                           tstamps_of_interest=None):
        assert(isinstance(self.response, Response))
        self.med_ret = MediaRetriever(self.response.url)

        dump_video = (self.request.get("dump_video") or dump_video) and \
            self.med_ret.is_video
        dump_images = (self.request.get("dump_images") or dump_images) or \
            self.med_ret.is_image

        if dump_images is False and self.med_ret.is_image:
            dump_images = True

        if dump_video is False and dump_images is False:
            log.warning("`dump_video` and `dump_images` are both false."
                        " Unable to proceed.")
            return

        log.debug(f"Dumping Video: {dump_video}")
        log.debug(f"Dumping Frames: {dump_images}")

        # we get the frame iterator
        frames_iterator = []
        if tstamps_of_interest is not None:
            if type(tstamps_of_interest) is list:
                for t in tstamps_of_interest:
                    frame = self.med_ret.get_frame(tstamp=t)
                    frames_iterator.append((frame, t))
        else:
            try:
                frames_iterator = self.med_ret.get_frames_iterator(
                    sample_rate=1.0
                )
            except Exception:
                log.error(traceback.format_exc())
                raise Exception("Error loading media")

        vid_file, images_dir, max_tstamp = self.dump_data(
            frames_iterator,
            dump_video=dump_video,
            dump_images=dump_images
        )

        props = []
        if self.local_dir is not None and dump_video:
            local_vid_path = self.copy_video(vid_file.name)
            p = Property(
                server=self.name,
                ver=self.version,
                value=local_vid_path,
                property_type="dumped_video",
                property_id=4,
            )
            props.append(p)

        if self.local_dir is not None and dump_images:
            local_frames_paths = self.copy_frames(images_dir.name)
            ps = [Property(
                server=self.name,
                ver=self.version,
                value=path,
                property_type="dumped_image",
                property_id=3,
            ) for path in local_frames_paths]
            props.extend(ps)

        if self.s3_bucket is not None and dump_video:
            s3_vid_url = self.upload_video(vid_file.name)
            p = Property(
                server=self.name,
                ver=self.version,
                value=s3_vid_url,
                property_type="dumped_video",
                property_id=2,
            )
            props.append(p)

        if self.s3_bucket is not None and dump_images:
            s3_frames_urls = self.upload_frames(images_dir.name)
            ps = [Property(
                server=self.name,
                ver=self.version,
                value=url,
                property_type="dumped_image",
                property_id=1,
            ) for url in s3_frames_urls]
            props.extend(ps)

        images_dir.cleanup()
        vid_file.close()

        media_summary = VideoAnn(t1=0.0, t2=max_tstamp, props=props)
        self.response.append_media_summary(media_summary)

    def copy_video(self, video_file_path):
        assert(os.path.isfile(video_file_path))

        media_id = self.create_media_id()
        filename = f"{media_id}.mp4"
        target_dir = os.path.join(self.local_dir, media_id)
        target_filepath = os.path.join(target_dir, filename)

        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        shutil.copyfile(video_file_path, target_filepath)
        return target_filepath

    def copy_frames(self, image_directory):
        assert(os.path.isdir(image_directory))

        media_id = self.create_media_id()
        target_dir = os.path.join(self.local_dir, media_id, "frames")

        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        for image_path in iglob(os.path.join(image_directory, "*")):
            image_name = os.path.basename(image_path)
            target_filepath = os.path.join(target_dir, image_name)
            shutil.copyfile(image_path, target_filepath)

        return target_dir

    def upload_video(self, video_file_path):
        assert(os.path.isfile(video_file_path))

        media_id = self.create_media_id()
        s3_key = f"{self.s3_key_prefix}/{media_id}/{media_id}.mp4"

        client = boto3.client("s3")
        client.upload_file(
            video_file_path,
            self.s3_bucket,
            s3_key,
            ExtraArgs={
                'ContentType': 'video/mp4'
            }
        )

        s3_url = client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.s3_bucket,
                'Key': s3_key
            },
            ExpiresIn=3600
        )
        return s3_url.split("?")[0]

    def upload_frames(self, image_directory):
        assert(os.path.isdir(image_directory))

        media_id = self.create_media_id()
        s3_key_prefix = f"{self.s3_key_prefix}/{media_id}/frames"
        client = boto3.client("s3")

        s3_urls = []
        for image_path in iglob(os.path.join(image_directory, "*")):
            image_name = os.path.basename(image_path)
            s3_key = f"{s3_key_prefix}/{image_name}"
            client.upload_file(
                image_path,
                self.s3_bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'image/jpeg'
                }
            )

            s3_url = client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.s3_bucket,
                    'Key': s3_key
                },
                ExpiresIn=3600
            )
            s3_urls.append(s3_url.split("?")[0])

        return s3_urls

    def create_media_id(self):
        media_id = os.path.basename(self.response.url).rsplit(".", 1)[0]

        media_id = "".join(
            [e for e in media_id if e.isalnum() or e in ["/", "."]]
        )
        hash = hashlib.md5(self.med_ret.url.encode()).hexdigest()
        return f"{media_id}_{hash}"

    def dump_data(self,
                  frames_iterator,
                  dump_video=False,
                  dump_images=False):
        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thickness = 2
        fps = 1

        vid = None

        image_dir = TemporaryDirectory()
        vid_file = NamedTemporaryFile(suffix=".mp4")

        tstamps = set(self.response.get_timestamps())
        tstamp_frame_anns = set(self.response.get_timestamps_from_frames_ann())
        video_width, video_height = self.med_ret.get_w_h()

        for i, (img, tstamp) in enumerate(frames_iterator):
            if vid is None:
                fourcc = cv2.VideoWriter_fourcc(*'H264')
                vid = cv2.VideoWriter(
                    vid_file.name, fourcc, fps, (video_width, video_height)
                )

            if tstamp in tstamp_frame_anns:
                log.debug(f"drawing frame for tstamp: {tstamp}")
                regions = self.response.get_regions_from_tstamp(tstamp)
                for region in regions:
                    rand_color = random_bgr()
                    p0, p1 = p0p1_from_bbox_contour(
                        region['contour'], video_width, video_height
                    )
                    anchor_point = [p0[0] + 3, p1[1] - 3]

                    if abs(p1[1] - video_height) < 30:
                        anchor_point = [p0[0] + 3, int(p1[1] / 2) - 3]

                    img = cv2.rectangle(img, p0, p1, rand_color, thickness)

                    prop_strs = get_props_from_region(region)
                    for i, prop in enumerate(prop_strs):
                        img = cv2.putText(
                            img,
                            prop,
                            (anchor_point[0], anchor_point[1] + i * 25),
                            face,
                            1.0,
                            rand_color,
                            thickness
                        )
            elif tstamp in tstamps:
                log.debug(f"Making frame at {tstamp} gray")
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                img = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
            else:
                log.debug(f"No frame at {tstamp}")
                continue

            img = cv2.putText(
                img,
                str(tstamp),
                (20, 20),
                face, scale,
                [255, 255, 255],
                thickness
            )

            if dump_video:
                vid.write(img)

            if dump_images:
                cv2.imwrite(f"{image_dir.name}/{tstamp}.jpeg", img)

        vid.release()
        return vid_file, image_dir, tstamp
