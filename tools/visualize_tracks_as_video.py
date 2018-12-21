import os
import sys
import argparse

import cv2

from cvapis.avro_api.avro_api import AvroIO, AvroAPI
from cvapis.avro_api.utils import p0p1_from_bbox_contour
from cvapis.comm_apis.work_handler import WorkerManager
from cvapis.media_api.media import MediaRetriever

from colour import Color
import random

import hashlib
import json
from tqdm import tqdm
import numpy as np

class VideoBuilder():
    def __init__(self, n=1, work_queue_name="VideoBuilder"):
        self.colors = ['darkorchid', 'darkgreen', 'coral', 'darkseagreen', 
        'forestgreen', 'firebrick', 'olivedrab', 'steelblue', 'tomato', 'yellowgreen']
        self.color_idx = 0
        self.default_fps = 60

        # if n > 0:
        # self.work_manager = WorkerManager(n=n, q_name=work_queue_name, parallelization='redis')
        # else:
        #     self.work_manager = WorkerManager()
        
        # self.write_manager = WorkerManager(self.write_frame_to_video)
        
    def get_fps(self, med_ret):
        fps = med_ret.get_fps()
        if not fps:
            fps = self.default_fps
        return fps

    def write_frames_to_video(self, frame):
        pass

    def add_regions_to_im(self, im, region_data, w_pad, h_pad):
        h, w, _ = im.shape

        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.5
        thickness = 1

        (_, inc), _ = cv2.getTextSize("|", face, scale, thickness)
        inc += 3
        y = inc

        for idx, region_datum in enumerate(region_data):
            x = w
            region, bgr_color, track_name, track_props = region_datum
            p0, p1 = p0p1_from_bbox_contour(region['contour'], w-w_pad, h-h_pad)
            im = cv2.rectangle(im, p0, p1, bgr_color)
            im = cv2.putText(im, str(idx), (p0[0]+3, p1[1]-3), face, scale, bgr_color, thickness)

            track_s = []
            for prop in track_props:
                track_s.append('{} ({:1.2f})'.format(prop['value'], prop['confidence']))
            track_s = 'Track: '+' | '.join(track_s)

            det_s = []
            for prop in region['props']:
                det_s.append('{} ({:1.2f})'.format(prop['value'], prop['confidence']))
            det_s = 'Detection: '+' | '.join(det_s)

            ss = [track_name, track_s, det_s]
            for s in ss:
                (s_w, _), _ = cv2.getTextSize(s, face, scale, thickness)
                x = min(x, w-s_w-5)

            # Track name
            im = cv2.putText(im, track_name, (x, y), face, scale, bgr_color, thickness)
            im = cv2.putText(im, str(idx)+":", (x-30, y), face, scale, bgr_color, thickness)
            y += inc

            # Det Pred
            im = cv2.putText(im, det_s, (x, y), face, scale, bgr_color, thickness)
            y += inc

            # Track Pred
            im = cv2.putText(im, track_s, (x, y), face, scale, bgr_color, thickness)
            y += 2*inc
        return im 

    @staticmethod
    def pad_image(im, w_pad=0, h_pad=0):
        if w_pad > 0:
            w_padding = np.zeros((im.shape[0], w_pad, im.shape[2])).astype('uint8')
            im = np.hstack((im, w_padding))
        if h_pad > 0:
            h_padding = np.zeros((h_pad, im.shape[1]+h_pad, im.shape[2])).astype('uint8')
            im = np.vstack((im, h_padding))
        return im

    def build_video(self, doc, write_name, fps=None, width_padding=200, height_padding=0, server=""):
        self.write_video_name = write_name+'.mov'
        color_map = {}

        avro_api = AvroAPI(doc)
        avro_api.sort_image_anns_by_timestamp()
        avro_api.sort_tracks_summary_by_timestamp()
        med_ret = MediaRetriever(avro_api.get_url().replace('/home/fjm/data/sports/videos/nhlGoldStandard','/home/matthew/NFS_Store/ComputerVision/sports/nhlGoldStandard'))

        if not fps:
            fps = self.get_fps(med_ret)

        w,h = med_ret.get_w_h()
        w += width_padding
        h += height_padding
        out_video = cv2.VideoWriter(self.write_video_name, cv2.VideoWriter_fourcc('D', 'I', 'V', 'X'), fps, (w, h))

        # for tstamp in tqdm(avro_api.get_timestamps()):
            # frame = add_boxes_to_frame(tstamp, avro_api, med_ret)
            # im = med_ret.get_frame(tstamp=tstamp)
        tstamps = set([float('%.3f'%(x)) for x in avro_api.get_timestamps()])
        tstamp_map = dict([(float('%.3f'%(x)), x) for x in avro_api.get_timestamps()])
        t = 0
        dt = 30
        for im, tstamp in med_ret.get_frames_iterator(med_ret.get_fps()):
            if not tstamp in tstamps:
                continue
            tstamp = tstamp_map[tstamp]
            im = self.pad_image(im, width_padding, height_padding)
            region_data = []
            for track in avro_api.get_sorted_tracks_from_timestamp(tstamp):
                if not track['props'][0]['server'] == server:
                    continue
                region_ids = track['region_ids']
                track_hash = hashlib.sha1(json.dumps(track, sort_keys=True).encode()).hexdigest()
                if not color_map.get(track_hash):
                    color_map[track_hash] = [x*255 for x in Color(self.colors[self.color_idx]).rgb[::-1]]
                    self.color_idx = (self.color_idx + 1)%len(self.colors)

                region = avro_api.get_region_from_region_ids_and_tstamp(region_ids, tstamp)
                if not region:
                    continue
                region_data.append((region, color_map[track_hash], track_hash, track['props']))
            im = self.add_regions_to_im(im, region_data, width_padding, height_padding)
            out_video.write(im)
            if tstamp > t:
                print('{} Seconds Processed'.format(t))
                t+=dt
        out_video.release()
        # cv2.destroyAllWindows()

if __name__=="__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--json", type=str, help="path to avro json doc")
    # a.add_argument("--labels", nargs='+', help="optional arg, list of labels of interest")
    # a.add_argument("--track_limit", help="max # of tracks to visualize PER LABEL")
    # a.add_argument("--min_conf", default=0.0, help="min conf of tracks to visualize")
    # a.add_argument("--start_time", default=0.0, help="time from which to start viz in seconds")
    a.add_argument("--video_name", type=str, help="where to write output track viz")
    # a.add_argument("--track_pad", default=0.0, help="num seconds to pad tracks for viz")
    # a.add_argument("--random", action="store_true", help="flag to select random tracks")
    # a.add_argument("--min_track_length", default=1.0, help="min track length for viz")
    a.add_argument("--fps", type=float, help="fps of writen video")
    a.add_argument("--w_padding", type=int, default=600, help="width padding for legend")
    a.add_argument("--h_padding", type=int, default=0, help="height padding for legend")
    a.add_argument("--server", type=str, default="HAM", help="server to visualize")
    args = a.parse_args()

    vb = VideoBuilder()
    vb.build_video(AvroIO.read_json(args.json),
                     args.video_name,
                     fps=args.fps,
                     width_padding=args.w_padding,
                     height_padding=args.h_padding,
                     server=args.server)
