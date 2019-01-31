from abc import ABC, abstractmethod


class Response(ABC):
    @abstractmethod
    def __init__(self, doc=None, request=None):
        pass
    
    @abstractmethod
    def set_doc(self, doc):
        pass

    @abstractmethod
    def response_to_mediadata(properties_of_interest=None):
        pass
    
    @abstractmethod
    def mediadata_to_response(self, module_data):
        pass
    
    @abstractmethod
    def to_bytes(self):
        pass
    
    @abstractmethod
    def to_dict(self):
        pass

    @abstractmethod
    def get_url(self):
        pass