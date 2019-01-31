from abc import abstractmethod

import glog as log

from vitamincv.module import Module


class PropertiesModule(Module):
    def process(self, request, prev_media_data=None):
        super().process(request, prev_media_data)
        self.process_properties()
        return "SUCCESS"

    @abstractmethod
    def process_properties(self):
        """Abstract method to be implemented to the child PropertiesModule, which appends to

        self.segments
        """
        pass
