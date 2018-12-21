import context
from cvapis.avro_api.cv_schema_factory import *
from cvapis.avro_api.avro_api import AvroIO, AvroAPI

import os
import csv



import glog as log




def append_trackssummary_from_csv(input_json_filepath,input_csv_filepath,output_json_filepath):
    if(os.path.exists(input_json_filepath)==False):
        log.warning(input_json_filepath + " doesn't exist")
        return
    if(os.path.exists(input_csv_filepath)==False):
        log.warning(input_csv_filepath + " doesn't exist")
        return
    filename = os.path.basename(input_json_filepath)
    filename =  os.path.splitext(filename)[0]
    #we load the input json
    avro_api = AvroAPI(AvroIO.read_json(input_json_filepath))
    #we parse the csv
    cois=[ 'Sponsor', 'Placement', 'Start Timestamp', 'Stop Timestamp']
#    cois=[ 'Sponsor', 'Location', 'Timestamp Start', 'Timestamp Stop']
    isponsor=-1
    iplacement=-1
    it1=-1
    it2=-1
    with open(input_csv_filepath) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        titles=[]
        placements=[]
        sponsors=[]
        t1s=[]
        t2s=[]
        
        for i,row in enumerate(readCSV):
            if i==0:
                titles=row
                titles =[t.strip() for t in titles]
                log.info("titles: " + str(titles))
                log.info("Getting indexes of interest for columns named:  " + str(cois))
                try:
                    isponsor=titles.index(cois[0])
                except:
                    isponsor=titles.index('Brand')
                try:
                    iplacement=titles.index(cois[1])
                except:
                     iplacement=titles.index('Location')
                try:
                    it1=titles.index(cois[2])
                except:
                    it1=titles.index('Timestamp Start')
                try:
                    it2=titles.index(cois[3])
                except:
                    it2=titles.index('Timestamp Stop')
                    
                log.info("indexes: " + str(isponsor) + ", " + str(iplacement) +", " + str(it1) +", " + str(it2))
            else:
                placements.append(row[iplacement])
                sponsors.append(row[isponsor])                
                t1aux=row[it1]
                t2aux=row[it2]
                if t1aux.find(':')==1:
                    c1 = t1aux.split(":")
                    c2 = t2aux.split(":")
                    #log.info("c1: " + str(c1))
                    #log.info("c2: " + str(c2))
                    #if len(c1)==1:
                    #    log.warning("Empty c1")
                    #    continue
                    #if len(c2)==1:
                    #    log.warning("Empty c2")
                    #    continue
                    #if len(c1[0])==0 or len(c1[1])==0 or len(c1[2])==0 or len(c2[0])==0 or len(c2[1])==0 or len(c2[2])==0:
                    #    log.warning("Misformed timestamp")
                    #    continue
                    #log.info("c1: " + str(c1[0])+","+ str(c1[1])+","+ str(c1[2]))
                    #log.info("c2: " + str(c2[0])+","+ str(c2[1])+","+ str(c2[2]))
                    #log.info("c2: " + str(c2))
                    try:
                        #hacking a problem in some timestamps. We assume here no video will be as long as 12h.
                        if c1[0]==12:
                            c1[0]=0
                        if c2[0]==12:
                            c2[0]=0
                        t1 = int(c1[0])*3600+ int(c1[1])*60+ int(c1[2])
                        t2 = int(c2[0])*3600+ int(c2[1])*60+ int(c2[2])
                    except:
                        log.warning("Misformed timestamp")
                        log.warning(t1aux)
                        log.warning(t2aux)
                        continue
                else:#we assume its seconds
                    try:
                        t1=int(t1aux)
                        t2=int(t2aux)
                    except:
                        log.warning("Misformed timestamp")
                        log.warning(t1aux)
                        log.warning(t2aux)
                        continue
                t1s.append(t1)
                t2s.append(t2)

        #log.info("placements[0:9]: " + str(placements[0:9]))
        #log.info("sponsors[0:9]: " + str(sponsors[0:9]))
        #log.info("t1s[0:9]: " + str(t1s[0:9]))
        #log.info("t2s[0:9]: " + str(t2s[0:9]))
        #log.info("placements[-9:]: " + str(placements[-9:]))
        #log.info("sponsors[-9:]: " + str(sponsors[-9:]))
        #log.info("t1s[-9:]: " + str(t1s[-9:]))
        #log.info("t2s[-9:]: " + str(t2s[-9:]))
    
    for t1,t2,placement,sponsor in zip(t1s,t2s,placements,sponsors):
        #we create the placecement property
        p1=create_prop(confidence=1,ver="1.0",server="HAM",property_type="placement",value=placement)
        #we create the sponsor property
        p2=create_prop(confidence=1,ver="1.0",server="HAM",property_type="logo",value=sponsor)
        ps= [p1,p2]
        #We create the track
        track=create_video_ann(t1=t1,t2=t2,props=ps)
        avro_api.append_track_to_tracks_summary(track)
    AvroIO.write_json(avro_api.get_response(), output_json_filepath, indent=2)
    assert(True)
    print("done")

def append_trackssummary_to_goldstrandard():
    #videos_list='/home/fjm/sandbox/nhllogoclassifier/list_nhlgoldstandard.txt'
    #jsons_folder='/home/fjm/sandbox/nhllogoclassifier/output_jsons_v1.0/'
    #jsons_folder='/home/fjm/nfs_drive/ComputerVision/sports/nhlplacementAndLogoJsonsv1.4/'
    jsons_folder='/home/fjm/sandbox/cvapis/tests/data/output_nhl_historical/20181026/'
    videos_list=jsons_folder + '/list_videos.txt'
    #csvs_folder='/home/fjm/nfs_drive/ComputerVision/sports/nhlGoldStandard/'
    #csvs_folder='/home/fjm/nfs_drive/ComputerVision/sports/csvbackup/'
    #csvs_folder='/home/fjm/nfs_drive/ComputerVision/sports/newcsvs/'
    csvs_folder=jsons_folder
    with open(videos_list) as f:
        content = f.readlines()
        content = [x.strip().strip("\"") for x in content]

    input_jsons=[]
    output_jsons=[]
    input_csvs=[]
    
    for x in content:
        log.info("x: " + x)
        filename = os.path.basename(x)
        log.info("filename: " + filename)
        filename_woext=os.path.splitext(filename)[0]
        log.info("filename_woext: " + filename_woext)
        input_json_filepath =jsons_folder+filename_woext + ".json"
        output_json_filepath=jsons_folder+filename_woext + "_With_Human_Tracks.json"
        input_csv_filepath=csvs_folder + filename_woext + ".csv"
        
        input_jsons.append(input_json_filepath)
        input_csvs.append(input_csv_filepath)
        output_jsons.append(output_json_filepath)

    for jsonin,csvin,jsonout in zip(input_jsons,input_csvs,output_jsons):
        log.info("----------------------------")
        log.info("Processing : " + jsonin)
        append_trackssummary_from_csv(jsonin,csvin,jsonout)
    
if __name__=="__main__":
    #########################
    # append_trackssummary_to_goldstrandard()
    input_json_filepath="/mnt/ldrive/gg/cvapis/tests/json_output/20181108/Winnipeg%20Jets%20%40%20St.%20Louis%20Blues-uvr287fay9k.json"
    input_csv_filepath="/mnt/ldrive/gg/cvapis/tests/json_output/20181108/csv/Winnipeg%20Jets%20%40%20St.%20Louis%20Blues-uvr287fay9k.csv"
    output_json_filepath="/mnt/ldrive/gg/cvapis/tests/json_output/20181108/csv/Winnipeg%20Jets%20%40%20St.%20Louis%20Blues-uvr287fay9k_With_Human_Tracks.json"
    append_trackssummary_from_csv(input_json_filepath,input_csv_filepath,output_json_filepath)
    
    
