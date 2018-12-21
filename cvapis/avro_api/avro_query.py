import numbers
import numpy as np
import GPUtil
import collections
from cvapis.avro_api.cv_schema_factory import *
import glog as log

import importlib.util

class AvroQuerier():
    def __init__(self):
        self.query_map = {}
        self.numeric_fields = set()
        self.xp = np
        self.max_gpu_mem = 0.5
        self.check_gpu()

    def load(self, json_list):
        """ Loads a list of detections or segments for preprocessing

            Uses GPU if one is available
        """
        log.debug("Loading json_list: "+ str(json_list))
        self.query_map = {}
        self.numeric_fields = set()
        if len(json_list)==0:
            return
        if json_list[0].get("t") is not None:
            json_list = sorted(json_list, key=lambda d: d["t"])

        if json_list[0].get("t2") is not None:
            json_list = sorted(json_list, key=lambda d: d["t2"])

        if json_list[0].get("t1") is not None:
            json_list = sorted(json_list, key=lambda d: d["t1"])

        self.full_set = set(range(len(json_list)))
        self.data = np.array(json_list)
        log.debug("Processing list")
        self._process_list(self.data, self.query_map)

        
        for args in self.numeric_fields:
            array = self.query_map
            update = {}
            for arg in args:
                log.debug('arg: ' + str(arg))
                array = array[arg]
                if type(array) is dict:
                    update[arg] = {}
            if self.gpu is not None:
                with self.xp.cuda.Device(self.gpu):
                    update[arg] = self.xp.array(array)
            else:
                update[arg] = self.xp.array(array)
            log.debug("Updating dictionary with update.keys(): " + str(update.keys()))
            self.update_dict(self.query_map, update)

    @staticmethod
    def intersect(old, new):
        idxs = old.intersection(new) if old is not None else new
        return idxs

    @staticmethod
    def union(old, new):
        idxs = old.union(new) if old is not None else new
        return idxs

    @staticmethod
    def difference(full, sub):
        idxs = full.difference(sub) if full is not None else sub
        return idxs

    def update_dict(self, old, new):
        for key, val in new.items():
            if isinstance(val, collections.Mapping):
                old[key] = self.update_dict(old.get(key, {}), val)
            else:
                old[key] = val
        return old


    def query(self, query):
        """ Queries for matching detections/segments

        Args:
            query: An AvroQuery or AvroQueryBlock object

        Returns:
            list: A list detections/segments
        """
        if type(query) is not AvroQuery and type(query) is not AvroQueryBlock:
            return

        idxs = self.process_qblock(query) if type(query) is AvroQueryBlock else self.process_q(query)
        return self.data[sorted(idxs)]

    def group_query(self, queries, group_field):
        """Queries for detections/segments and groups by matching field names

        Args:
            queries (list): A list of AvroQuery and/or AvroQueryBlock object to query with
            group_field (str): The detection/segment field group by

        Returns:
            list[tuple]: A list of grouped detections/segments
        """
        qresults = []
        matching_ids = None
        for query in queries:
            _qresults = {}
            qresult = self.query(query)
            for d in qresult:
                group_id = d[group_field]
                if _qresults.get(group_id) is None:
                    _qresults[group_id] = []
                _qresults[group_id].append(d)
            matching_ids = self.intersect(matching_ids, set(_qresults.keys()))
            qresults.append(_qresults)


        results = []
        for key in matching_ids:
            result = []
            for qresult in qresults:
                result += qresult[key]
            results.append(tuple(result))

        return results

    def process_qblock(self, query):
        operation = query.get_operation(query.operation_code)
        idxs = None
        
        for q in query.query:
            if type(q) is AvroQueryBlock:
                _idxs = self.process_qblock(q)
            elif type(q) is AvroQuery:
                _idxs = self.process_q(q)

            if operation == "AND":
                idxs = self.intersect(idxs, _idxs)
            elif operation == "OR":
                idxs = self.union(idxs, _idxs)

        if not query.include:
            idxs = self.difference(self.full_set, idxs)

        if idxs is None:
            idxs = set()

        return idxs

    def process_q(self, query):
        log.debug('Processing query')
        idxs = None
        skip = set(["include"])
        for key, val in query.__dict__.items():
            #log.debug('key, val: ' + str(key) +', ' + str(val))
            if key in skip:
                continue
            if callable(val):
                continue
            if val is None:
                continue
            
            if type(val) is str:
                #log.debug('type(val) is str')
                _idxs = self.string_query(key, val)
                #log.debug('_idxs: ' + str(_idxs))

            elif isinstance(val, numbers.Number) and "min_" in key:
                maximum = getattr(query, key.replace("min_", "max_"))
                skip.add(key.replace("min_", "max_"))
                
                if maximum is None:
                    maximum = self.xp.inf

                field = key.replace("min_", "")
                _idxs = self.numeric_range_query(val, maximum, field)

            elif isinstance(val, numbers.Number) and "max_" in key:
                minimum = getattr(query, key.replace("max_", "min_"))
                skip.add(key.replace("max_", "min_"))
                
                if minimum is None:
                    minimum = -1*self.xp.inf

                field = key.replace("max_", "")
                _idxs = self.numeric_range_query(minimum, val, field)

            elif isinstance(val, numbers.Number):
                _idxs = self.numeric_match_query(val, key)

            if idxs is None:
                idxs = _idxs
            else:
                idxs = self.intersect(idxs, _idxs)

        if idxs is None:
            idxs = set()

        if not query.include:
            idxs = self.difference(self.full_set, idxs)

        return idxs

    def string_query(self, *args):
        _result = self.query_map
        #log.debug('_result: ' + str(_result))
        for arg in args:
            log.debug('arg: ' + str(arg))
            _result = _result[arg]
        return _result

    def numeric_range_query(self, minimum=None, maximum=None, *args):
        if minimum is None:
            minimum = -1*self.xp.inf
        if maximum is None:
            maximum = self.xp.inf

        qmap = self.query_map
        for arg in args:
            qmap = qmap[arg]
        result = set(self.xp.nonzero(self.xp.logical_and(minimum <= qmap, qmap <= maximum))[0].tolist())
        return result

    def numeric_match_query(self, value, *args):
        _result = self.query_map
        for arg in args:
            _result = _result[arg]
        result = set(self.xp.nonzero(_result == value)[0].tolist())
        return result

    def check_gpu(self):
        self.gpu = None
        deviceIDs = []
        try:
            deviceIDs = GPUtil.getAvailable(order="memory", maxMemory = self.max_gpu_mem, maxLoad = 0.70)
        except:
            log.warning("No GPUs Found -- This must be a CPU only machine")

        if len(deviceIDs) > 0:
            if importlib.util.find_spec("cupy"):
                import cupy as cp
                self.xp = cp
                self.gpu = deviceIDs[0]


    """ Detections/Segments Loading """
    def create_sub_dict(self, full_dict, *args):
        sub_dict = full_dict
        for arg in args:
            if sub_dict.get(arg) is None:
                sub_dict[arg] = {}
            sub_dict = sub_dict[arg]
        return sub_dict

    def _process_dict(self, list_idx, elem, sub_dict, *args, prev_args=()):
        sub_dict = self.create_sub_dict(sub_dict, *args)
        for key, val in elem.items():
            if type(val) is list:
                self._process_list(val, sub_dict, key, idx=list_idx, prev_args=prev_args+args)
            elif type(val) is dict:
                self._process_dict(list_idx, val, sub_dict, key, prev_args=prev_args+args)
            elif type(val) is str:
                self._process_val_str(list_idx, sub_dict, key, val)
            elif isinstance(val, numbers.Number):
                self._process_val_num(list_idx, val, sub_dict, key, prev_args=prev_args+args)

    def _process_list(self, val, sub_dict, *args, idx=None, prev_args=()):
        sub_dict = self.create_sub_dict(sub_dict, *args)
        for _idx, elem in enumerate(val):
            if type(elem) is dict:
                if idx is None:
                    self._process_dict(_idx, elem, sub_dict, prev_args=prev_args+args)
                else:
                    self._process_dict(idx, elem, sub_dict, prev_args=prev_args+args)

    def _process_val_str(self, idx, sub_dict, *args):
        sub_dict = self.create_sub_dict(sub_dict, *args[:-1])
        val = args[-1]
        if type(sub_dict.get(val)) is not set:
            sub_dict[val] = set()

        sub_dict[val].add(idx)

    def _process_val_num(self, idx, val, sub_dict, *args, prev_args=()):
        sub_dict = self.create_sub_dict(sub_dict, *args[:-1])
        key = args[-1]
        if type(sub_dict.get(key)) is not list:
            sub_dict[key] = []

        for _ in  range(idx-len(sub_dict[key]) if idx-len(sub_dict[key]) > 0 else 0):
            sub_dict[key].append(np.nan)

        sub_dict[key].append(val)
        self.numeric_fields.add(prev_args+args)


class AvroQueryBlock():
    """ Enables more advanced queries using multiple AvroQuery objects

    """
    def __init__(self):
        self.query = []
        self.operation_code = 1
        self.include = True

    def get_operation(self, operation=None):
        """ Gets current method for aggregating queries or translates an int/str to an operation str/int

            Args:
                operation (int|str): desired operation to have translated
        """
        if operation is None:
            operation = self.operation_code

        if type(operation) is str:
            if operation == "AND":
                return 0

            if operation == "OR":
                return 1

        if type(operation) is int:
            if operation == 0:
                return "AND"

            if operation == 1:
                return "OR"


    def set_operation(self, operation = "OR"):
        """ Sets operation

            Args:
                operation (int|str): desired operation to switch to

            "AND" and 0 mean all added queries must True to return detection/segement
            "OR" and 1 mean one added queries must True to return detection/segement
        """
        if type(operation) is str:
            self.operation_code = self.get_operation(operation)
        elif type(operation) is int:
            self.operation_code = operation

    def add(self, query):
        """ Appends AvroQuery or AvroQueryBlock object for querying
        """
        if not type(query) is AvroQuery and not type(query) is AvroQueryBlock:
            return

        self.query.append(query)

    def set_include(self, include=True):
        self.include = include

    def set_exclude(self, exclude=True):
        self.include = not exclude

class AvroQuery():
    """ Basic Query Object

        Parses default detection and segment from cv_schema_factory to find queriable fields

    """
    def __init__(self):
        self.include = True

        for key, val in create_detection().items():
            self._init_process_dict_items(key, val)

        for key, val in create_segment().items():
            self._init_process_dict_items(key, val)

    def set(self, query_dict):
        """ Allows passing dictionaries instead of manually "match" and "range" methods

            Args:
                query_dict (dict): A dictionary encoding how to perform query
        """
        for key, val in query_dict.items():
            if hasattr(self, key) and not callable(getattr(self, key)):
                setattr(self, key, val)

    def _init_process_dict_items(self, key, val):
        """ Dynamically creates functions to setup queries

            Args:
                key (str): Field in detection/segment 
                val (str/Number/list): The default value for the key in the detection/segment

            Creates:
                Callable methods "match_"+key that allows explicit matching 
                        between keys and values in the set of detections

                If val is a number, also create methods "set_min_"+key and "set_max_"+key to
                        enable range queries

            Example: 
                # If detection has field "t" and it is a number, and field "server" that's a string the following is possible...

                Q = AvroQuery()
                Q.match_server("HAM")
                Q.set_min_t(5)
                Q.set_max_t(15)
        """
        if type(val) is list:
            self._init_process_list(val)
            return
        setattr(self, key, None)
        self._match_constructor(key)
        if isinstance(val, numbers.Number):
            self._range_constructor(key)

    def _init_process_list(self, value):
        for elem in value:
            if type(elem) is not dict:
                continue
            for key, val in elem.items():
                self._init_process_dict_items

    def _match_constructor(self, attr):
        """ Dynamically creates "match" methods

            Args:
                attr (str): The detection/segment field to query against
        """
        def _match_template(value):
            setattr(self, attr, value)

        setattr(self, "match_"+attr, _match_template)

    def _range_constructor(self, attr):
        """ Dynamically creates "range" methods

            Args:
                attr (str): The detection/segment field to query against
        """
        def _min_range_template(value):
            setattr(self, "min_"+attr, value)

        def _max_range_template(value):
            setattr(self, "max_"+attr, value)

        setattr(self, "min_"+attr, None) 
        setattr(self, "max_"+attr, None)
        setattr(self, "set_min_"+attr, _min_range_template)
        setattr(self, "set_max_"+attr, _max_range_template)

    def set_include(self, include=True):
        self.include = include

    def set_exclude(self, exclude=True):
        self.include = not exclude
