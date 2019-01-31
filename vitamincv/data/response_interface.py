from abc import ABC, abstractmethod


class Response(ABC):
    def __init__(self, response=None, request=None):
        self.set_response(response)
        self.request = request

    @abstractmethod
    def set_response(self, response):
        pass

    @abstractmethod
    def to_mediadata(properties_of_interest=None):
        """ Abstract method that converts and then returns 
            this response to a MediaData object

        Args:
            properties_of_interest (dict): dict containing property values that
                                           will be passed
        
        Returns:
            MediaData:
        """
        pass

    @abstractmethod
    def load_mediadata(self, media_data):
        """Abstract method that loads mediadata into this Response object

        Args:
            media_data (MediaData)
        """
        pass

    @abstractmethod
    def to_bytes(self):
        pass

    @abstractmethod
    def to_dict(self):
        pass

    def get_request(self):
        return self.request
