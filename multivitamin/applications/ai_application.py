import json
from abc import (ABC, abstractmethod)

import glog as log
from flask import jsonify

from multivitamin.data import Request, Response


class AiApplication(ABC):
    '''
        This class represent an AI Application. An AI Application is a list of modules to
        iterate over.
    '''
    models = None  # Where we keep the model when it's loaded

    @abstractmethod
    def get_modules(self):
        pass

    def predict(self, input):
        try:
            req = Request(input)
            response = Response(req)

            for module in self.get_modules():
                log.debug(f"Processing request for module: {module}")
                response = module.process(response)
                log.debug(f"response.to_dict(): {json.dumps(response.to_dict(), indent=2)}")

            if req.bin_encoding:
                return response.to_bytes()
            else:
                return jsonify(response.to_dict())
        except Exception:
            log.exception(f"Error processing request {input}")
        return jsonify({"error": "Could not process this request"})