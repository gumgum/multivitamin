import os
import sys
import glog as log
from collections import defaultdict

import context
from vitamincv.module_api.cvmodule import CVModule

class DummyDetector(CVModule):
    def __init__(self, server_name, version, net_data_dir):
        self.request_api=None
        super().__init__(server_name, version)
        log.info("Dummy constructor")
    def process(self, message):
        self.set_message(message)
        log.info("Dummy process")
    def update_response(self):
        log.info("Dummy update_response")
