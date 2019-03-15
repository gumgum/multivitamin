from abc import ABC, abstractmethod


class CommAPI(ABC):
    @abstractmethod
    def pull(self):
        pass

    @abstractmethod
    def push(self):
        pass
