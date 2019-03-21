# from multivitamin.data.response.data import MediaAnn, Point, ResponseInternal
# import json

# p = Point(1, 2)
# print(p)
# print(type(p))
# print(type(Point))
# assert isinstance(p, type(Point()))

# m = MediaAnn(w=44)
# print(type(m))
# print(m['w'])
# print(json.dumps(m.to_dict(), indent=2))
# x = m.to_dict()
# print(x)


# ri = ResponseInternal()

# ve = ri.to_dict()
# print(ve)
# maa = ResponseInternal.from_dict(ri.to_dict())
# # print(maa)
# # # x['z'] = 2.0
# # # print(x)
# # # # x['w'] = 33
# # import random

from collections import MutableMapping

from dataclasses import dataclass, field, asdict
from typing import List
from typeguard import typechecked
# from mashumaro import DataClassDictMixin


class DictLike(MutableMapping):
    """Base class used for the below dataclasses, so that each dataclass can 
    act like a dict with [] access
    """
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
        return f"{super().__repr__()}, ({self.__dict__})"



@typechecked
@dataclass
class VideoAnn(DictLike):
    t1: float = 0.0
    t2: float = 0.0
    props: List[int] = field(default_factory=list)
    regions: List[float] = field(default_factory=list)
    region_ids: List[str] = field(default_factory=list)

v = VideoAnn()
print(v)
dv = asdict(v)
print(dv)
xe = VideoAnn(**dv)
print(xe)