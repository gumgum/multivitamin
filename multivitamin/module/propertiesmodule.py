from abc import abstractmethod

import glog as log

from multivitamin.module import Module


class PropertiesModule(Module):
    def __init__(
        self,
        server_name,
        version,
        prop_type=None,
        prop_id_map=None,
        module_id_map=None,
    ):
        """Module that processes properties, not images
        
        Args:
            server_name (str): server_name
            version (str): version
            prop_type (str, optional): Defaults to None. property_type
            prop_id_map (str, optional): Defaults to None. Map: property_type -> id
            module_id_map (str, optional): Defaults to None. Map: server_name -> id
        """
        super().__init__(
            server_name=server_name,
            version=version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,
        )
        log.debug("Creating PropertiesModule")

    def process(self, response):
        """Process method called by Server

        Args:
            response (Response): prev_response
        
        Returns:
            Response: resultant response
        """
        super().process(response)
        self.process_properties()
        return self.update_and_return_response()

    @abstractmethod
    def process_properties(self):
        """Abstract method to be implemented to the child PropertiesModule

        Should populate response with self.response.append_track(...) 
        or self.response.append_media_summary(...)
        """
        pass
