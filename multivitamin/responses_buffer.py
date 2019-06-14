from multivitamin.data import Response
from threading import Lock
import glog as log

class ResponsesBuffer():
    def __init__(self,n,enable_parallelism):
        self._n=n
        self._enable_parallelism=enable_parallelism
        self._buffer=[]
        self._lock = Lock()

    def is_parallelism_enabled(self):
        return self._enable_parallelism

    def get_stats(self):
        self._lock.acquire()
        stats = self._get_stats()        
        self._lock.release()
        return stats

    def _get_stats(self):
        stats={}
        for response in self._buffer:
           state=response. _check_response_state()
           count=stats.get(state.name,0)
           count = count +1
           stats[state.name]=count
        return stats

    def get_current_number_responses(self):
        n=0
        self._lock.acquire()
        n= len(self._buffer)
        self._lock.release()
        return n

    def get_required_number_requests(self):
        n=0
        self._lock.acquire()
        n = self._n - len(self._buffer)
        self._lock.release()
        return n

    
    def add_response(self,response):
        self._lock.acquire()        
        response._fetch_media()
        self._buffer.append(response)
        self._lock.release()

    def get_responses_ready_to_be_processed(self,nmax=None):
        responses=[]
        n=self._n
        if nmax and nmax<self._n:
            n=nmax
        self._lock.acquire()
        for response in self._buffer:
            if response.is_ready_to_be_processed():
                responses.append(response)
            if len(responses)>=n:
                break
        self._lock.release()
        return responses

    def get_responses_to_be_pushed(self):
        responses=[]
        self._lock.acquire()
        for response in self._buffer:
            if response.is_already_processed():
                responses.append(response)
        self._lock.release()
        return responses

    def get_all_responses(self):
        responses=[]
        self._lock.acquire()
        for response in self._buffer:
            responses.append(response)
        self._lock.release()
        return responses

    def clean_pushed_responses(self):
        responses=[]     
        self._lock.acquire()
        buffer_aux = [x for x in self._buffer if not x.is_pushed()]
        n_del = len(self._buffer) - len(buffer_aux)
        self._buffer=buffer_aux
        self._lock.release()
        return n_del

