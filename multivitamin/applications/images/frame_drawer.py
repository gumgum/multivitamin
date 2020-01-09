import os
import shutil
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
import hashlib


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

        video_width, video_height = self.med_ret.get_w_h()
        media_id = os.path.basename(self.response.url).rsplit(".", 1)[0]

        media_id = "".join(
            [e for e in media_id if e.isalnum() or e in ["/", "."]]
        )
        content_type_map = {}

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

        dump_folder = f"{self.local_dir}/"
        self.dumping_folder_url = dump_folder

        if dump_folder and not os.path.exists(dump_folder):
            os.makedirs(dump_folder)

        hash = hashlib.md5(self.med_ret.url.encode()).hexdigest()
        if dump_video:
            filename = os.path.join(dump_folder, f"{media_id}_{hash}.mp4")
            fps = 1
            frameSize = (video_width, video_height)
            fourcc = cv2.VideoWriter_fourcc(*'H264')

            log.debug("filename: " + filename)
            log.debug("fourcc: " + str(fourcc))
            log.debug("type(fourcc): " + str(type(fourcc)))
            log.debug("fps: " + str(fps))
            log.debug("type(fps): " + str(type(fps)))
            log.debug("frameSize: " + str(frameSize))
            log.debug("type(frameSize): " + str(type(frameSize)))

            vid = cv2.VideoWriter(filename, fourcc, fps, frameSize)
            content_type_map[os.path.basename(filename)] = 'video/mp4'
        elif dump_images and self.med_ret.is_image:
            filename = dump_folder + media_id + "_" + hash + ".jpeg"
        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thickness = 2

        # we get the image_annotation tstamps
        tstamps = self.response.get_timestamps()
        tstamp_frame_anns = self.response.get_timestamps_from_frames_ann()
        log.debug('tstamps: ' + str(tstamps))
        log.debug('tstamps_dets: ' + str(tstamp_frame_anns))

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

        for i, (img, tstamp) in enumerate(frames_iterator):
            if img is None or tstamp is None:
                m = f"frame at tstamp={tstamp}" if img is None else "tstamp"
                log.warning(f"Invalid {m}")
                continue

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
                log.debug("Making frame gray")
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                img = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
            else:
                log.debug("No processed frame")
                continue

            # Add timestamp to frame
            img = cv2.putText(
                img,
                str(tstamp),
                (20, 20),
                face, scale,
                [255, 255, 255],
                thickness
            )

            if dump_video:
                # we add the frame
                log.debug("Adding frame")
                vid.write(img)

            if dump_images:
                # we dump the frame
                if self.med_ret.is_video:
                    filename = os.path.join(
                        dump_folder,
                        f"{media_id}_{tstamp}.jpg"
                    )

                log.debug(f"Writing to file: {filename}")
                cv2.imwrite(filename, img)

                content_type_map[os.path.basename(filename)] = 'image/jpeg'

        if dump_video:
            vid.release()

        s3_url_root = None
        if self.s3_bucket:
            s3_url_root = self.upload_files(
                dump_folder, content_type_map, media_id
            )

        if self.local_dir == DEFAULT_DUMP_FOLDER:
            log.info('Removing files in ' + dump_folder)
            shutil.rmtree(dump_folder)

        props = []
        if dump_images:
            val = filename

            if self.med_ret.is_video:
                val = f"{dump_folder}/{media_id}_*.jpg"

            props.append(
                Property(
                    server=self.name,
                    ver=self.version,
                    value=val,
                    property_type="dumped_images",
                    property_id=1,
                )
            )
        if dump_video and s3_url_root is not None:
            dumped_video_url = os.path.basename(filename)
            props.append(
                Property(
                    server=self.name,
                    ver=self.version,
                    value=f"{s3_url_root}/{dumped_video_url}",
                    property_type="dumped_video",
                    property_id=2,
                )
            )
        media_summary = VideoAnn(t1=0.0, t2=tstamp, props=props)
        self.response.append_media_summary(media_summary)

    def upload_files(self, source, content_type_map, media_id):
        log.info("Uploading files")
        session = boto3.Session()
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.s3_bucket)
        key_root = f"{self.s3_key_prefix}/{media_id}"
        # https://<bucket-name>.s3.amazonaws.com/<key>
        s3_url_root = \
            f"https://s3.amazonaws.com/{self.s3_bucket}/{key_root}"

        for subdir, dirs, files in os.walk(source):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    rel_path = os.path.basename(full_path)
                    key = f"{key_root}/{rel_path}"
                    log.info('Pushing ' + full_path + ' to ' + s3_url_root)
                    try:
                        content_type = content_type_map[
                            os.path.basename(full_path)
                        ]
                    except Exception:
                        # File is not intended to be uploaded.
                        # It was not generated in this execution.
                        content_type = None

                    if content_type:
                        bucket.put_object(
                            Key=key, Body=data, ContentType=content_type
                        )

        return s3_url_root
