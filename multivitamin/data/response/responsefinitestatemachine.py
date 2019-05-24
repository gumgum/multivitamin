from threading import Lock


class ResponseFiniteStateMachine(ABC):
    def __init__(
        self, enablefsm=False        
    ):
    self.enabled=enablefsm
    #we instanciate the lock
    self._lock = Lock()

    def enablefsm(self):
        self.enabled=True

    class States(Enum):
        IRRELEVANT=1
        TO_BE_PROCESSED=2
        PREPARING_TO_BE_PROCESSED=3
        READY_TO_BE_PROCESSED=4
        BEING_PROCESSED=5
        PROCESSED=6

    #Updating or checking the state of whichever response is controlled by a lock
    def _update_response_state(self,state):
        ret=True
        if self.enabled==False:
            return        
        self._lock.acquire()
        if self.state==state:
            ret=False
        self.state=state
        self._lock.release()
        return ret
    def _check_response_state(self):
        if self.enabled==False:
            return States.IRRELEVANT
        self._lock.acquire()
        state=self.state
        self._lock.release()
        return state

    ################
    def is_to_be_processed(self,response):
        if self.enabled==False:
            return True
        return self.self_check_response_state()==States.TO_BE_PROCESSED
    def is_preparing_to_be_processed(self,response):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.PREPARING_TO_BE_PROCESSED
    def is_ready_to_be_processed(self,response):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.READY_TO_BE_PROCESSED   
    def is_already_processed(self,response):
        if self.enabled==False:
            return True
        return self._check_response_state()==States.PROCESSED
    #################
    def set_as_to_be_processed(self):
        return self._update_response_state(States.TO_BE_PROCESSED)
    def set_as_preparing_to_be_processed(self,response):
        return self._update_response_state(States.PREPARING_TO_BE_PROCESSED)   
    def set_as_ready_to_be_processed(self):
        return self._update_response_state(States.READY_TO_BE_PROCESSED)
    def set_as_being_processed(self):
        return self._update_response_state(States.BEING_PROCESSED)
    def set_as_processed(self):
        self._update_response_state(States.PROCESSED)   
    ##############################