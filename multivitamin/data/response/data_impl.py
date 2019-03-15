from collections import MutableMapping
import json


class DictLike(MutableMapping):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

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

    def __str__(self):
        return str(self.__dict__)
        
    def __repr__(self):
        return f'{super().__repr__()}, ({self.__dict__})'
    
    def to_json(self, indent=2):
        """Serialize to JSON string

        Returns:
            str: serialized JSON str
        """
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=indent
        )