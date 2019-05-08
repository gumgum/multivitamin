import os
import sys
import shutil
import random
import json
import traceback

import boto3
import glog as log
import cv2
from colour import Color

from multivitamin.module import PropertiesModule
from multivitamin.data import Response
from multivitamin.data.response.utils import p0p1_from_bbox_contour, get_current_time
from multivitamin.data.response.dtypes import Property, VideoAnn
from multivitamin.media import MediaRetriever


COLORS = [
    'darkorchid', 'darkgreen', 'coral', 'darkseagreen',
    'forestgreen', 'firebrick', 'olivedrab', 'steelblue',
    'tomato', 'yellowgreen'
]


def get_rand_bgr():
    color = Color(COLORS[random.randint(0, len(COLORS)-1)]).rgb[::-1]
    return [x*255 for x in color]


def get_props_from_region(region):
    prop_strs = []
    for prop in region["props"]:
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
        #log.setLevel('DEBUG')
        self.response = None
        if isinstance(response, Response):
            self.response = response
        elif isinstance(response, dict):
            self.response = Response(response)
        else:
            log.debug("No response")

        self.med_ret = med_ret
        if med_ret is None:
            if self.response is not None:
                self.med_ret = MediaRetriever(self.response.url)

        if s3_bucket and not s3_key:
            raise ValueError("s3 bucket defined but s3 key not defined")
        if s3_key and not s3_bucket:
            raise ValueError("s3 key defined but s3 bucket not defined")
        if not pushing_folder and not s3_key:
            raise ValueError("pushing_folder and s3 key not defined, we cannot set where to dump.")
        if pushing_folder:
            self.pushing_folder = pushing_folder
        else:
            self.pushing_folder = DEFAULT_DUMP_FOLDER
            
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def process_properties(self, dump_video=True, dump_images=False,tstamps_Of_Interest=None):
        self.last_tstamp = 0.0
        assert(self.response)
        self.med_ret = MediaRetriever(self.response.url)

        self.w, self.h = self.med_ret.get_w_h()
        media_id = os.path.basename(self.response.url).rsplit(".", 1)[0]
        self.media_id = "".join([e for e in media_id if e.isalnum() or e in ["/", "."]])
        self.content_type_map = {}
        
        # if there is no flag in the request of not request_api we'll get None.
        self.dump_video = None
        self.dump_images = None
        try:
            self.dump_video = self.request.get("dump_video")
            self.dump_images = self.request.get("dump_images")
        except Exception:
            log.error("Unable to get flags from request dump_video or dump_images")
            pass
        
        if self.dump_video is None:
            self.dump_video = dump_video
        if self.dump_images is None:
            self.dump_images = dump_images
        if self.dump_video is False and self.dump_images is False:
            log.warning("Not dumping anything--you might want to dump something.")
            return

        dump_folder = self.pushing_folder + '/' + self.media_id + '/'
        self.dumping_folder_url = dump_folder
        if dump_folder:
            if not os.path.exists(dump_folder):
                os.makedirs(dump_folder)

        if self.dump_video:
            filename = dump_folder + '/video.mp4'
            fps = 1
            frameSize = self.med_ret.shape
            frameSize = (self.w, self.h)
            fourcc = cv2.VideoWriter_fourcc(*'H264')
            log.info("filename: " + filename)
            log.info("fourcc: " + str(fourcc))
            log.info("type(fourcc): " + str(type(fourcc)))
            log.info("fps: " + str(fps))
            log.info("type(fps): " + str(type(fps)))
            log.info("frameSize: " + str(frameSize))
            log.info("type(frameSize): " + str(type(frameSize)))
            vid = cv2.VideoWriter(filename, fourcc, fps, frameSize)
            self.content_type_map[os.path.basename(filename)] = 'video/mp4'
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
        if tstamps_Of_Interest:
            if type(tstamps_Of_Interest) is list:
                for t in tstamps_Of_Interest:
                    frame=self.med_ret.get_frame(tstamp=t)
                    frames_iterator.append((frame,t))
        elif tstamps_Of_Interest is None:
            try:
                frames_iterator = self.med_ret.get_frames_iterator(sample_rate=1.0)
            except Exception:
                log.error(traceback.format_exc())
                raise Exception("Error loading media")

        for i, (img, tstamp) in enumerate(frames_iterator):
            self.last_tstamp = tstamp
            if img is None:
                log.warning("Invalid frame")
                continue
            if tstamp is None:
                log.warning("Invalid tstamp")
                continue
            # log.info('tstamp: ' + str(tstamp))
            if tstamp in tstamp_frame_anns:
                log.debug("drawing frame for tstamp: " + str(tstamp))
                # we get image_ann for that time_stamps 
                regions = self.response.get_regions_from_tstamp(tstamp)
                # log.info(json.dumps(image_ann, indent=2))
                for region in regions:
                    rand_color = get_rand_bgr()
                    p0, p1 = p0p1_from_bbox_contour(region['contour'], self.w, self.h)
                    anchor_point = [p0[0]+3, p1[1]-3]
                    if abs(p1[1]-self.h) < 30:
                        anchor_point = [p0[0]+3, int(p1[1]/2)-3]
                    img = cv2.rectangle(img, p0, p1, rand_color, thickness)
                    prop_strs = get_props_from_region(region)
                    for i, prop in enumerate(prop_strs):
                        img = cv2.putText(
                            img,
                            prop,
                            (anchor_point[0], anchor_point[1]+i*25),
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
            # Include the timestamp
            img = cv2.putText(img, str(tstamp), (20, 20), face, scale, [255, 255, 255], thickness)
            if self.dump_video:
                # we add the frame
                log.debug("Adding frame")
                vid.write(img)
            if self.dump_images:
                # we dump the frame
                outfn = "{}/{}.jpg".format(dump_folder, tstamp)
                log.debug("Writing to file: {}".format(outfn))
                cv2.imwrite(outfn, img)
                self.content_type_map[os.path.basename(outfn)] = 'image/jpeg'

        if self.dump_video:
            vid.release()
        if self.s3_bucket:
            try:
                self.upload_files(dump_folder)
            except Exception:
                log.error(traceback.format_exc())
        if self.pushing_folder == DEFAULT_DUMP_FOLDER:
            log.info('Removing files in '+dump_folder)
            shutil.rmtree(dump_folder)

        props = []
        if self.dump_images:
            props.append(
                Property(
                    server=self.name,
                    ver=self.version,
                    value=self.dumping_folder_url,
                    property_type="dumped_images",
                    property_id=1,
                )
            )
        if self.dump_video:
            dumped_video_url = self.dumping_folder_url + '/video.mp4'
            dumped_video_url = dumped_video_url.replace('//', '/')
            props.append(
                Property(
                    server=self.name,
                    ver=self.version,
                    value=dumped_video_url,
                    property_type="dumped_video",
                    property_id=2,
                )
            )
        media_summary = VideoAnn(t1=0.0, t2=self.last_tstamp, props=props)
        self.response.append_media_summary(media_summary)

    def upload_files(self, path):
        log.info("Uploading files")
        session = boto3.Session()
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.s3_bucket)
        key_root = self.s3_key + '/' + self.media_id + '/'
        # https://<bucket-name>.s3.amazonaws.com/<key>
        self.dumping_folder_url = 'https://s3.amazonaws.com/' + self.s3_bucket + '/' + key_root

        for subdir, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    rel_path = os.path.basename(full_path)
                    key = key_root + rel_path
                    log.info('Pushing ' + full_path + ' to ' + self.dumping_folder_url)
                    try:
                        content_type = self.content_type_map[os.path.basename(full_path)]
                    except Exception:
                        content_type = None #file is not intended to be uploaded, it was not generated in this execution.
                    if content_type:
                        bucket.put_object(Key=key, Body=data, ContentType=content_type)
