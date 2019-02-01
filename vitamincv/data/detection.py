import inspect
import numpy as np

from vitamincv.data.contour import Contour
from vitamincv.data.limited_dict import LimitedDict


class Detection(LimitedDict):
    def __init__(self, *args, **kwargs):
        # Avro JSON Properties
        self._server = None
        self._module_id = None
        self._property_type = None
        self._value = None
        self._value_verbose = None
        self._confidence = None
        self._fraction = None
        self._t = None
        self._contour = None
        self._ver = None
        self._region_id = None
        self._property_id = None
        self._footprint_id = None
        self._company = None

        self._server_default = ""
        self._module_id_default = 0
        self._property_type_default = "label"
        self._value_default = ""
        self._value_verbose_default = ""
        self._confidence_default = 0.0
        self._fraction_default = 1.0
        self._t_default = 0.0
        self._contour_default = Contour(box=[(0, 0), (1, 1)])
        self._ver_default = ""
        self._region_id_default = ""
        self._property_id_default = 0
        self._footprint_id_default = ""
        self._company_default = "gumgum"

        # Bonus attributes
        # self._bbox = None
        # self._mask = None
        self._frame = None

        super(Detection, self).__init__(*args, **kwargs)

    def box(self):
        p0, p1 = self.contour.box()
        if self.frame():
            w, h, c = self.frame.shape
            p0.x = p0.x * (w - 1)
            p0.y = p0.y * (h - 1)
            p1.x = p1.x * (w - 1)
            p1.y = p1.y * (h - 1)
        return p0, p1

    def frame(self):
        return self._frame

    def crop(self):
        p0, p1 = self.box()
        return self.frame[p0.x : p1.x, p0.y : p1.y].copy()

    ##############
    # PROPERTIES #
    ##############

    # Server
    @property
    def server(self):
        return self._server_default if self._server is None else self._server

    @server.setter
    def server(self, server):
        assert isinstance(server, str)
        self._server = server

    @server.deleter
    def server(self):
        self._server = None

    # Module Id
    @property
    def module_id(self):
        return self._module_id_default if self._module_id is None else self._module_id

    @module_id.setter
    def module_id(self, module_id):
        assert isinstance(module_id, int)
        self._module_id = int(module_id)

    @module_id.deleter
    def module_id(self):
        self._module_id = None

    # Property Type
    @property
    def property_type(self):
        return self._property_type_default if self._property_type is None else self._property_type

    @property_type.setter
    def property_type(self, property_type):
        assert isinstance(property_type, str)
        self._property_type = property_type

    @property_type.deleter
    def property_type(self):
        self._property_type = None

    # Value
    @property
    def value(self):
        return self._value_default if self._value is None else self._value

    @value.setter
    def value(self, value):
        assert isinstance(value, str)
        self._value = value

    @value.deleter
    def value(self):
        self._value = None

    # Value Verbose
    @property
    def value_verbose(self):
        return self._value_verbose_default if self._value_verbose is None else self._value_verbose

    @value_verbose.setter
    def value_verbose(self, value_verbose):
        assert isinstance(value_verbose, str)
        self._value_verbose = value_verbose

    @value_verbose.deleter
    def value_verbose(self):
        self._value_verbose = None

    # Confidence
    @property
    def confidence(self):
        return self._confidence_default if self._confidence is None else self._confidence

    @confidence.setter
    def confidence(self, confidence):
        self._confidence = float(confidence)

    @confidence.deleter
    def confidence(self):
        self._confidence = None

    # Fraction
    @property
    def fraction(self):
        return self._fraction_default if self._fraction is None else self._fraction

    @fraction.setter
    def fraction(self, fraction):
        fraction = float(fraction)
        # fraction must be between 0 and 1
        fraction = np.clip(fraction, 0.0, 1.0)
        self._fraction = fraction

    @fraction.deleter
    def fraction(self):
        self._fraction = None

    # Tstamp
    @property
    def t(self):
        return self._t_default if self._t is None else self._t

    @t.setter
    def t(self, t):
        self._t = float(t)

    @t.deleter
    def t(self):
        self._t = None

    # Contour
    @property
    def contour(self):
        return self._contour_default if self._contour is None else self._contour

    @contour.setter
    def contour(self, contour):
        assert isinstance(contour, Contour)
        self._contour = contour

    @contour.deleter
    def contour(self):
        self._contour = None

    # Version
    @property
    def ver(self):
        return self._ver_default if self._ver is None else self._ver

    @ver.setter
    def ver(self, ver):
        assert isinstance(ver, str)
        self._ver = ver

    @ver.deleter
    def ver(self):
        self._ver = None

    # Region Id
    @property
    def region_id(self):
        return self._region_id_default if self._region_id is None else self._region_id

    @region_id.setter
    def region_id(self, region_id):
        assert isinstance(region_id, str)
        self._region_id = region_id

    @region_id.deleter
    def region_id(self):
        self._region_id = None

    # Property Id
    @property
    def property_id(self):
        return self._property_id_default if self._property_id is None else self._property_id

    @property_id.setter
    def property_id(self, property_id):
        self._property_id = int(property_id)

    @property_id.deleter
    def property_id(self):
        self._property_id = None

    # Footprint Id
    @property
    def footprint_id(self):
        return self._footprint_id_default if self._footprint_id is None else self._footprint_id

    @footprint_id.setter
    def footprint_id(self, footprint_id):
        assert isinstance(footprint_id, str)
        self._footprint_id = footprint_id

    @footprint_id.deleter
    def footprint_id(self):
        self._footprint_id = None

    # Company
    @property
    def company(self):
        return self._company_default if self._company is None else self._company

    @company.setter
    def company(self, company):
        assert isinstance(company, str)
        self._company = company

    @company.deleter
    def company(self):
        self._company = None
