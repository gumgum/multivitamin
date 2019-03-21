from abc import ABC, abstractmethod


class CommAPI(ABC):
    """Abstract base class to define an interface of `push()` and `pull()`"""
    @abstractmethod
    def pull(self):
        pass

    @abstractmethod
    def push(self):
        pass
