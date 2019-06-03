from threading import Lock
from abc import ABC, abstractmethod
from multivitamin.media import OpenCVMediaRetriever
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
    BEING_PUSHED=7
    PUSHED=8

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

        self._pushing_thread=None
        self._pushing_thread_creation_time=None
        self._pushing_thread_timeout=900#seconds

    def enablefsm(self):
        self.enabled=True
        if self.is_irrelevant():
            self.set_as_to_be_processed()
        
    def get_lifetime_downloading_thread(self):
        if self._downloading_thread_creation_time:
            lifetime=time.time()- self._downloading_thread_creation_time
            return lifetime
        return 0

    def get_lifetime_pushing_thread(self):
        if self._pushing_thread_creation_time:
            lifetime=time.time()- self._pushing_thread_creation_time
            return lifetime
        return 0

    def check_timeouts(self):
        try:
            self.check_timeout_downloading_thread()
            self.check_timeout_pushing_thread()
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())


    def check_timeout_downloading_thread(self):        
        if not self.enabled:
            return False
        if not self.is_preparing_to_be_processed():
            return False
        lifetime=self.get_lifetime_downloading_thread()
        if lifetime>self._downloading_thread_timeout:
            log.debug("self._downloading_thread_creation_time: " + str(self._downloading_thread_creation_time))
            log.debug("lifetime: " + str(lifetime))            
            log.error("ERROR_TIMEOUT. The downloading thread HAS BEEN ALIVE FOR TOO LONG")
            return True

    def check_timeout_pushing_thread(self):        
        if not self.enabled:
            return False
        if not self.is_being_pushed():
            return False
        lifetime=self.get_lifetime_pushing_thread()
        if lifetime>self._pushing_thread_timeout:
            log.debug("self._pushing_thread_creation_time: " + str(self._pushing_thread_creation_time))
            log.debug("lifetime: " + str(lifetime))            
            log.error("ERROR_TIMEOUT. The pushing thread HAS BEEN ALIVE FOR TOO LONG")
            return True

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

    def is_being_pushed(self):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.BEING_PUSHED

    def is_pushed(self):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.PUSHED

    def _check_response_state(self):        
        log.debug(self._state.name +'.'+ self.url)
        if self.enabled==False:
            return States.IRRELEVANT
        self._lock.acquire()
        state=self._state
        self._lock.release()
        return state

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
    def set_as_being_pushed(self):
        return self._update_response_state(States.BEING_PUSHED)
    def set_as_pushed(self):
        return self._update_response_state(States.PUSHED)

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

    def _fetch_media(self,media_retriever_type=OpenCVMediaRetriever):
        if not self.set_as_preparing_to_be_processed():
            return
        if self.enabled:
            self._downloading_thread_creation_time=time.time()
            log.info("Creating thread at " + str(self._downloading_thread_creation_time))            
            self._downloading_thread=Thread(group=None, target=ResponseFiniteStateMachine._fetch_media_thread_safe, name=None, args=(self,media_retriever_type), kwargs={})
            self._downloading_thread.start()
            log.info("Thread created")            
        else:            
            self._fetch_media(r)

    @staticmethod
    def _fetch_media_thread_safe(response,media_retriever_type):
        """Fetches the media from response.request.url
           Careful, you must keep this method threadsafe
        """
        try:
            if not response.media:
                log.debug(f"Loading media from url: {response.request.url}")
                response.media = media_retriever_type(response.request.url) 
            else:
                log.debug(f"media from url: {response.request.url} was already in place.")
        except Exception as e:
            log.error(e)
            log.error(traceback.print_exc())
            response.code = Codes.ERROR_LOADING_MEDIA            
        lifetime=response.get_lifetime()
        response.set_as_ready_to_be_processed()
        log.debug('Total lifetime: ' + str(lifetime) + ', ' + response.request.url)
        return

    def _push(self,output_comms):        
        if not self.set_as_being_pushed():
            return
        if self.enabled:            
            for o in output_comms:                
                if inspect.ismethod(type(o).push_thread_safe) and inspect.ismethod(type(o).prepare_parameters_for_push_thread_safe):
                    output_comms_thread_safe.append(o)
                else:
                    output_comms_non_thread_safe.append(o)
            for o in output_comms_non_thread_safe:
                log.warning ("output_comms_non_thread_safe: " + str(type(o)) + ". Slowing down execution")
                o.push(responses=[self])
            if len(output_comms_thread_safe)>0:
                log.info("Launching one thread for " + str(output_comms_thread_safe))
                self._pushing_thread=Thread(group=None, target=ResponseFiniteStateMachine._push_thread_safe, name=None, args=(self,output_comms_thread_safe), kwargs={})
                self._pushing_thread.start()               
            else:
                self.set_as_pushed()
        else:
            for o in output_comms:
                o.push([self])
            self.set_as_pushed()

    @staticmethod
    def _push_thread_safe(response,output_comms):
        for output_comm in output_comms:
            parameters=output_comm.prepare_parameters_for_push_thread_safe(response)
            log.info("parameters: " + str(parameters))
            type(output_comm).push_thread_safe(parameters)
        response.set_as_pushed()
