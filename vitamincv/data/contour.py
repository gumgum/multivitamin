import numpy as np
from copy import copy

from vitamincv.data.limited_dict import LimitedDict
from vitamincv.metrics_api.distances import rectilinear_jaccard, rectilinear_intersection, rectilinear_union

class Point(LimitedDict):
    def __init__(self, *args, **kwargs):
        self._x = None
        self._y = None
        # self._z = None
        # self._t = None
        # self._pitch = None
        # self._yaw = None
        # self._roll = None
        super(Point, self).__init__(*args, **kwargs)

    def _set_dict_attr(self, name, value):
        if hasattr(self, name) and value is not None:
            super(LimitedDict, self).__setitem__(name, value)

    @property
    def x(self):
        return self._x

    @x.setter
    def x(self, x):
        x = np.clip(x, 0.0, 1.0)
        self._x = float(x)

    @x.deleter
    def x(self):
        self._x = None

    @property
    def y(self):
        return self._y

    @y.setter
    def y(self, y):
        y = np.clip(y, 0.0, 1.0)
        self._y = float(y)

    @y.deleter
    def y(self):
        self._y = None

    # @property
    # def z(self):
    #     return self._z
    #
    # @z.setter
    # def z(self, z):
    #     self._z = float(z)
    #
    # @z.deleter
    # def z(self):
    #     self._z = None


class Contour(list):
    def __init__(self, box=None, contour=None):
        if box:
            assert(isinstance(box, list))
            self._create_contour_from_2dbox(box)

        if contour:
            for element in contour:
                self.append(element)

    def _create_contour_from_2dbox(self, box):
        box = np.array(box).flatten()
        box = box.reshape(2, 2)
        pt_order = [(0,0), (1,0), (1,1), (0,1)]
        for idx, jdx in pt_order:
            pt = Point()
            pt.x = box[idx, 0]
            pt.y = box[jdx, 1]
            self.append(pt)

    def box(self):
        assert(len(self)>0)
        p0 = copy(self[0])
        p1 = copy(self[0])
        for pt in self:
            p0.x = min(p0.x, pt.x)
            p0.y = min(p0.y, pt.y)
            p1.x = max(p1.x, pt.x)
            p1.y = max(p1.y, pt.y)
        return p0, p1

    def append(self, element):
        if not isinstance(element, Point):
            try:
                element = Point(element)
            except:
                pass
        if isinstance(element, Point):
            super(Contour, self).append(element)
