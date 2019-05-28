from threading import Lock
from abc import ABC, abstractmethod
from multivitamin.media import MediaRetriever
from multivitamin.module.codes import Codes
from threading import Thread
import traceback

import time



from enum import Enum

import glog as log

class States(Enum):
    IRRELEVANT=1
    TO_BE_PROCESSED=2
    PREPARING_TO_BE_PROCESSED=3
    READY_TO_BE_PROCESSED=4
    BEING_PROCESSED=5
    PROCESSED=6

class ResponseFiniteStateMachine(ABC):
    def __init__(self,enablefsm=False):
        self.enabled=enablefsm
        #we instanciate the lock
        self._lock = Lock()
        self._state=States.IRRELEVANT
        self.media=None
        self._downloading_thread=None
        self._downloading_thread_creation_time=None
        self._downloading_thread_timeout=240#seconds
    def __del__(self): 
        self.stop_downloading_thread()
        

    def stop_downloading_thread(self):
        if self._downloading_thread:
            if not self._downloading_thread.is_alive():                
                return False
        log.warning('Stopping (NOT IMPLEMENTED) thread id: '+ str(self._downloading_thread.ident))
        return True
        #############        
        if self._downloading_thread:
            try:            
                #to be implemented if necessary
                pass
            except Exception as e:
                log.error("error exiting self._downloading_thread")
                log.error(traceback.format_exc())                
        log.info("Thread killed")
        self._downloading_thread=None
        self._downloading_thread_creation_time=None
        self.set_as_ready_to_be_processed()
        return True
        
    def get_lifetime(self):
         lifetime=time.time()- self._downloading_thread_creation_time
         return lifetime

    def check_timeout(self):        
        if not self.enabled:
            return False
        if not self.is_preparing_to_be_processed():
            return False
        lifetime=self.get_lifetime()
        if lifetime>self._downloading_thread_timeout:
            log.debug("self._downloading_thread_creation_time: " + str(self._downloading_thread_creation_time))
            log.debug("lifetime: " + str(lifetime))            
            self.stop_downloading_thread()
            log.error("ERROR_TIMEOUT. This thread HAVE BEEN ALIVE FOR TOO LONG")
            return True

    def enablefsm(self):
        self.enabled=True
        if self.is_irrelevant():
            self.set_as_to_be_processed()

    #Updating or checking the state of whichever response is controlled by a lock
    def _update_response_state(self,state):        
        ret=True
        if self.enabled==False:
            return ret
        self._lock.acquire()
        if self._state==state:
            ret=False
        else:
            log.debug('From ' + self._state.name + ' to '+ state.name +'.'+ self.url)
        self._state=state
        self._lock.release()
        return ret
    def _check_response_state(self):        
        log.debug(self._state.name +'.'+ self.url)
        if self.enabled==False:
            return States.IRRELEVANT
        self._lock.acquire()
        state=self._state
        self._lock.release()
        return state

    ################
    def is_irrelevant(self):
        return self._check_response_state()==States.IRRELEVANT
    def is_to_be_processed(self):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.TO_BE_PROCESSED
    def is_preparing_to_be_processed(self):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.PREPARING_TO_BE_PROCESSED
    def is_ready_to_be_processed(self):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.READY_TO_BE_PROCESSED   
    def is_already_processed(self):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.PROCESSED
    #################
    def set_as_to_be_processed(self):
        return self._update_response_state(States.TO_BE_PROCESSED)
    def set_as_preparing_to_be_processed(self):
        return self._update_response_state(States.PREPARING_TO_BE_PROCESSED)   
    def set_as_ready_to_be_processed(self):
        return self._update_response_state(States.READY_TO_BE_PROCESSED)
    def set_as_being_processed(self):
        return self._update_response_state(States.BEING_PROCESSED)
    def set_as_processed(self):
        self._update_response_state(States.PROCESSED)   
    ##############################
    def _download_media(self):
        if self.enabled:
            self._downloading_thread_creation_time=time.time()
            log.info("Creating thread at " + str(self._downloading_thread_creation_time))            
            self._downloading_thread=Thread(group=None, target=ResponseFiniteStateMachine._fetch_media, name=None, args=(self,), kwargs={})
            self._downloading_thread.start()
            log.info("Thread created")            
        else:
            self._fetch_media(r)        

    @staticmethod
    def _fetch_media(response):
        """Fetches the media from response.request.url
           Careful, you must keep this method threadsafe
        """
        try:
            log.debug("preparing response")
            if not response.media:
                log.debug(f"Loading media from url: {response.request.url}")
                response.media = MediaRetriever(response.request.url)
            else:
                log.debug(f"media from url: {response.request.url} was already in place.")
            ResponseFiniteStateMachine._parse_media(response)
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())
            response.code = Codes.ERROR_LOADING_MEDIA
            response.set_as_processed()#error downloading the image
        response.set_as_ready_to_be_processed()
        lifetime=response.get_lifetime()
        log.debug('Total lifetime: ' + str(lifetime) + ', ' + response.request.url)
        return

    @staticmethod
    def _parse_media(response):
        #if it is video or image, we get a frame iterator and we update width and height
        if response.media.is_image or response.media.is_video:
            response.frames_iterator = response.media.get_frames_iterator(
                response.request.sample_rate
            )             
        (width, height) = response.media.get_w_h()
        log.debug(f"Setting in response w: {width} h: {height}")
        response.width = width
        response.height = height