import os
import sys
import argparse
import cv2
from colour import Color
import random
import json
import glog as log
import traceback
import boto3
from vitamincv.module_api.cvmodule import CVModule
from vitamincv.avro_api.avro_api import AvroIO, AvroAPI
from vitamincv.avro_api.utils import p0p1_from_bbox_contour
from vitamincv.media_api.media import MediaRetriever
import cv2

COLORS = ['darkorchid', 'darkgreen', 'coral', 'darkseagreen', 
            'forestgreen', 'firebrick', 'olivedrab', 'steelblue', 
            'tomato', 'yellowgreen']

def get_rand_bgr():
    color = Color(COLORS[random.randint(0, len(COLORS)-1)]).rgb[::-1]
    return [x*255 for x in color]

def get_props_from_region(region):
    prop_strs = []
    for prop in region["props"]:
        out = "{}_{:.2f}".format(prop["value"], prop["confidence"])
        prop_strs.append(out)
    return prop_strs

class FrameDrawer(CVModule):
    def __init__(self, avro_api=None,med_ret=None,module_id_map=None,pushing_folder='./tmp', s3_bucket=None, s3_key=None):
        super().__init__(server_name='FrameDrawer', version='1.0.0', module_id_map=module_id_map)
        """Given an avro document, draw all frame_annotations
        
        Args:
        
        """
        if avro_api:
            self.avro_api=avro_api
        if self.avro_api==None:
            log.error('No avro_api')
            
        if med_ret:
            self.med_ret=med_ret
        else:
            self.med_ret = MediaRetriever(self.avro_api.get_url())            
        self.w, self.h = self.med_ret.get_w_h()
        media_id = os.path.basename(self.avro_api.get_url()).rsplit(".", 1)[0]
        self.media_id = "".join([e for e in media_id if e.isalnum() or e in ["/", "."]])
        

        if s3_bucket and not s3_key:
            raise ValueError("s3 bucket defined but s3 key not defined")
        if s3_key and not s3_bucket:
            raise ValueError("s3 key defined but s3 bucket not defined")
        if not pushing_folder and not s3_key:
            raise ValueError("pushing_folder and s3 key not defined, we cannot set where to dump.")
        self.pushing_folder = pushing_folder
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def process(self, dump_video=False, dump_images=False):
        if dump_video==False  and dump_images==False:
            log.warning("You may want to dump something.")
            return
                       
        dump_folder= self.pushing_folder + '/' + self.media_id +'/'
        if dump_folder:
            if not os.path.exists(dump_folder):
                os.makedirs(dump_folder)        
        if dump_video:
            filename = dump_folder + '/video.mp4'            
            fps =1
            frameSize=self.med_ret.shape
            frameSize=(self.w,self.h)            
            fourcc = cv2.VideoWriter_fourcc(*'H264')
            log.info("filename: " + filename)
            log.info("fourcc: " + str(fourcc))
            log.info("type(fourcc): " + str(type(fourcc)))                      
            log.info("fps: " + str(fps))
            log.info("type(fps): " + str(type(fps)))
            log.info("frameSize: " + str(frameSize))
            log.info("type(frameSize): " + str(type(frameSize)))
            vid=cv2.VideoWriter(filename, fourcc, fps,frameSize)
            
        face = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thickness = 2
                  
        #we get the image_annotation tstamps
        tstamps_dets=self.avro_api.get_timestamps()
        log.info('tstamps_dets: ' + str(tstamps_dets))
        #we get the frame iterator
        frames_iterator=[]
        try:
            frames_iterator=self.med_ret.get_frames_iterator(sample_rate=1.0)
        except:
            log.error(traceback.format_exc())
            exit(1)          
        
        for i, (img, tstamp) in enumerate(frames_iterator):
            if img is None:
                log.warning("Invalid frame")
                continue
            if tstamp is None:
                log.warning("Invalid tstamp")
                continue
	        #log.info('tstamp: ' + str(tstamp))
            if tstamp not in tstamps_dets:
                continue
            log.info("drawing frame for tstamp: " + str(tstamp))            
            #we get image_ann for that time_stamps
            image_ann=self.avro_api.get_image_ann_from_t(tstamp)
            log.debug(json.dumps(image_ann, indent=2))
            for region in image_ann["regions"]:
                rand_color = get_rand_bgr()
                p0, p1 = p0p1_from_bbox_contour(region['contour'], self.w, self.h)
                img = cv2.rectangle(img, p0, p1, rand_color, thickness)
                prop_strs = get_props_from_region(region)
                for i, prop in enumerate(prop_strs):
                    img = cv2.putText(img, prop, (p0[0]+3, p1[1]-3+i*25), face, 1.0, rand_color, thickness)
            #Include the timestamp
            img = cv2.putText(img, str(tstamp), (20, 20), face, scale, [255,255,255], thickness)
            if dump_video:
                #we add the frame
                log.debug("Adding frame")
                vid.write(img)
            elif dump_images:
                #we dump the frame
                outfn = "{}/{}.jpg".format(dump_folder, tstamp)
                log.debug("Writing to file: {}".format(outfn))
                cv2.imwrite(outfn, img)
        
        if  dump_video:            
            vid.release()                      
        if self.s3_bucket:
            self.upload_files(dump_folder)
    def upload_files(self,path):
        log.info("Uploading files")
        session = boto3.Session()
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.s3_bucket)
        for subdir, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    rel_path=os.path.basename(full_path)
                    key=self.s3_key + '/' +self.media_id + '/' + rel_path     
                    log.info('Pushing ' + full_path + ' to ' + key)
                    content_type=content_type_video                
                    bucket.put_object(Key=key, Body=data,ContentType='video/mp4')#image/jpeg
