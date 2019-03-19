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
        """[summary]
        
        Args:
            server_name (str): [description]
            version ([type]): [description]
            prop_type ([type], optional): Defaults to None. [description]
            prop_id_map ([type], optional): Defaults to None. [description]
            module_id_map ([type], optional): Defaults to None. [description]
        """
        super().__init__(
            server_name=server_name,
            version=version,
            prop_type=prop_type,
            prop_id_map=prop_id_map,
            module_id_map=module_id_map,
        )
        log.info("Creating PropertiesModule")

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

        Should populate response with self.response.append_track()
        """
        pass
