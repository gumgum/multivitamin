from time import sleep

class DummyCVModule():
    def __init__(self):
        self.message = None
        
    # def set_message(self, message):
    #     self.message=message

    def process(self, message):
        self.message = message
        sleep(3)
        return self.message