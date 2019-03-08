import json
import copy

from collections import MutableMapping

class Dictable(MutableMapping):
    def __setitem__(self, key, value):
        self.__dict__[key] = value
    def __getitem__(self, key):
        return self.__dict__[key]
    def __delitem__(self, key):
        del self.__dict__[key]
    def __iter__(self):
        return iter(self.__dict__)
    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        return self.__dict__
    
    def __str__(self):
        return str(self.__dict__)
