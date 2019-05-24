from threading import Lock


class FiniteStateMachine(ABC):
    def __init__(
        self, enabled=False        
    ):
    self.enabled=enabled
    if self.enabled:
        #we instanciate the lock
        self._lock = Lock()
    
    class States(Enum):
        IRRELEVANT=1
        TO_BE_PROCESSED=2
        PREPARING_TO_BE_PROCESSED=3
        READY_TO_BE_PROCESSED=4
        BEING_PROCESSED=5
        PROCESSED=6

    #Updating or checking the state of whichever response is controlled by a lock
    def _update_response_state(self,response,state):
        ret=True
        if self.enabled==False:
            return        
        self._lock.acquire()
        if response.state==state:
            ret=False
        response.state=state
        self._lock.release()
        return ret
    def _check_response_state(self,response):
        if self.enabled==False:
            return States.IRRELEVANT
        self._lock.acquire()
        state=response.state
        self._lock.release()
        return state

    ################
    def is_to_be_processed(self,response):
        if self.enabled==False:
            return True
        return self_check_response_state(response)==States.TO_BE_PROCESSED
    def is_preparing_to_be_processed(self,response):
        if self.enabled==False:
            return True
        return self._check_response_state(response)==States.PREPARING_TO_BE_PROCESSED
    def is_ready_to_processed(self,response):
        if self.enabled==False:
            return True
        return self._check_response_state(response)==States.READY_TO_BE_PROCESSED   
    def is_already_processed(self,response):
        if self.enabled==False:
            return True
        return self._check_response_state(response)==States.PROCESSED
    #################
    def set_as_to_be_processed(self,response):
        return self._update_response_state(response,States.TO_BE_PROCESSED)
    def set_as_preparing_to_be_processed(self,response):
        return self._update_response_state(response,States.PREPARING_TO_BE_PROCESSED)   
    def set_as_ready_to_be_processed(self,response):
        return self._update_response_state(response,States.READY_TO_BE_PROCESSED)
    def set_as_being_processed(self,response):
        return self._update_response_state(response,States.BEING_PROCESSED)
    def set_as_processed(self,response):
        self._update_response_state(response,States.PROCESSED)   
    ##############################