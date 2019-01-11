from pprint import pprint
import json

class QueryBuilder():
    def __init__(self, include_yaml=None, exclude_yaml=None, include = {}, exclude ={}):
        self.query = {
            "query":{}
        }
        self.root_path = "media_annotation"
        self.query_ = {}
        self.set_include = include
        self.set_exclude = exclude
        # self.include = {}
        # self.exclude = {}
        # if include_yaml:
        #     self.include = yaml.load(open(include_yaml))
        # if exclude_yaml:
        #     self.exclude = yaml.load(open(exclude_yaml))
        # self._construct_codes()

    def set(self, include={}, exclude={}):
        self.set_include = include
        self.set_exclude = exclude
        self.query = {
            "query":{}
        }
        self.query_ = {}

    def build(self):
        if type(self.set_include) == type({}) and type(self.set_exclude) == type({}):
            self.include = self.set_include.copy()
            self.exclude = self.set_exclude.copy()
            self._construct_query()
            self.query_ = self.query
        if type(self.set_include) == type([]) and type(self.set_exclude) == type({}):
            self.exclude = self.set_exclude.copy()
            for inc in self.set_include:
                self.include = inc
                self._construct_query()
                self.query_ = self.query_.get('query', {}).get('bool', {}).get('filter',[]) + self.query.get('query', {}).get('bool', {}).get('filter', [])
                self.query_ = {
                    "query":{
                        "bool":{
                            "filter":self.query_
                        }
                    }
                }
        return self.query_

    def _build_filter(self, parent_path, elem, subarrays=[], min_max_keys=[]):
        if not elem:
            return
        filter = []
        for key in self._desired_keys(elem.keys(), subarrays):
            if key in min_max_keys:
                continue
            # for e in elem[key]:
            field_path = parent_path + '.' + key
            field_path = field_path.strip('.')
            filter.append(self.filter_value(field_path, elem[key]))
        for idx, key in enumerate(min_max_keys[::2]):
            if not key in elem.keys():
                continue
            min = elem[min_max_keys[2*idx]]
            max = elem[min_max_keys[2*idx+1]]
            key = key.rsplit('_',1)[0]
            field_path = parent_path + '.' + key
            field_path = field_path.strip('.')
            if 'date' == key:
                min = str(min)
                max = str(max)
            filt = self.filter_range(field_path, min, max)
            if filt:
                filter.append(filt)
        return filter

    def _desired_keys(self, keys, subfields):
        desired_keys = set(keys) - set(subfields)
        return list(desired_keys)

    def _construct_query(self):
        include = self.include
        exclude = self.exclude
        subarrays = ['codes', 'frames_annotation','media_summary']
        min_max = ['h_min','h_max','w_min','w_max'] # Must be order [min, max, min, max,...]
        inc_filter = self._build_filter(self.root_path, include, subarrays, min_max)
        codes = self._construct_codes()
        frames_ann = self._construct_frames_ann()
        media_summary = self._construct_media_summary()
        if codes:
            inc_filter.append(codes)
        if frames_ann:
            inc_filter.append(frames_ann)
        if media_summary:
            inc_filter.append(media_summary)
        exc_filter = self._build_filter(self.root_path, exclude, subarrays, min_max)
        if inc_filter or exc_filter:
            bool_group = self.build_bool_group(inc_filter, exc_filter)
            self.query['query'].update(bool_group)

    def _construct_media_summary(self):
        field = 'media_summary'
        avro_path = self.root_path+"."+field
        subarrays = ['regions']
        min_max = ['t1_min', 't1_max','t2_min', 't2_max']
        include = self.include.get(field, {})
        exclude = self.exclude.get(field, {})
        inc_filter = self._build_filter(avro_path, include, subarrays, min_max)
        contour = self._construct_contour(field)
        if contour:
            inc_filter.append(contour)
        props = self._construct_properties(field)
        if props:
            inc_filter.append(props)
        exc_filter = self._build_filter(avro_path, exclude, subarrays, min_max)
        if inc_filter or exc_filter:
            bool_group = self.build_bool_group(inc_filter, exc_filter)
            media_summary_filter = self.filter_nested(avro_path, bool_group)
            return media_summary_filter

    def _construct_contour(self, parent_field):
        field = 'contour'
        avro_path = self.root_path + '.' +parent_field + '.regions.' + field
        subarrays = []
        min_max = ['x_min','x_max','y_min','y_max']
        include = self.include.get(parent_field, {}).get('regions',{}).get(field, {})
        exclude = self.exclude.get(parent_field, {}).get('regions',{}).get(field, {})
        inc_filter = self._build_filter(avro_path, include, subarrays, min_max)
        exc_filter = self._build_filter(avro_path, exclude, subarrays, min_max)
        if inc_filter or exc_filter:
            bool_group = self.build_bool_group(inc_filter, exc_filter)
            contour_filter = self.filter_nested(avro_path, bool_group)
            return contour_filter

    def _construct_properties(self, parent_field):
        field = 'props'
        avro_path = self.root_path + '.' + parent_field + '.regions.' + field
        subarrays = []
        min_max = ['confidence_min','confidence_max','confidence_min_min','confidence_min_max','fraction_min','fraction_max']
        include = self.include.get(parent_field, {}).get('regions',{}).get(field, {})
        exclude = self.exclude.get(parent_field, {}).get('regions',{}).get(field, {})
        inc_filter = self._build_filter(avro_path, include, subarrays, min_max)
        exc_filter = self._build_filter(avro_path, exclude, subarrays, min_max)
        if inc_filter or exc_filter:
            bool_group = self.build_bool_group(inc_filter, exc_filter)
            contour_filter = self.filter_nested(avro_path, bool_group)
            return contour_filter

    def _construct_frames_ann(self):
        field = 'frames_annotation'
        avro_path = self.root_path+"."+field
        subarrays = ['regions']
        min_max = ['t_min', 't_max']
        include = self.include.get(field, {})
        exclude = self.exclude.get(field, {})
        inc_filter = self._build_filter(avro_path, include, subarrays, min_max)
        contour = self._construct_contour(field)
        if contour:
            inc_filter.append(contour)
        props = self._construct_properties(field)
        if props:
            inc_filter.append(props)
        exc_filter = self._build_filter(avro_path, exclude, subarrays, min_max)
        if inc_filter or exc_filter:
            bool_group = self.build_bool_group(inc_filter, exc_filter)
            frames_ann_filter = self.filter_nested(avro_path, bool_group)
            return frames_ann_filter

    def _construct_codes(self):
        field = 'codes'
        avro_path = self.root_path+"."+field
        min_max = ['date_min','date_max']
        include = self.include.get(field, {})
        exclude = self.exclude.get(field, {})
        inc_filter = self._build_filter(avro_path, include, min_max_keys=min_max)
        exc_filter = self._build_filter(avro_path, exclude, min_max_keys=min_max)
        if inc_filter or exc_filter:
            bool_group = self.build_bool_group(inc_filter, exc_filter)
            codes_filter = self.filter_nested(avro_path, bool_group)
            return codes_filter
        # self.query['query'].update(self.codes_filter)

    def filter_range(self, path, min, max):
        if not path:
            return
        if min ==None and max==None:
            return 
        range = {
            "range":{
                path:{}
            }
        }
        if not min == None:
            range['range'][path]['gte'] = min
        if not max == None:
            range['range'][path]['lte'] = max
        return range

    def filter_value(self, path, value):
        if not path or not value:
            return
        match = {
            "match_phrase":{path:value}
        }
        return match

    def filter_nested(self, path, group):
        if not group or not path:
            return
        reg = {
            "nested":{
                "path": path,
                "query": group,
                "inner_hits":{}
            }
        }
        return reg

    def build_or_group(self, group):
        filt = {
            "bool":{
                "should":group
            }
        }
        return filt

    def build_bool_group(self, include, exclude):
        bool = {
            "bool":{}
        }
        if include:
            bool['bool'].update(self.build_filter_group(include))
        if exclude:
            bool['bool'].update(self.build_not_filter_group(exclude))
        return bool

    def build_filter_group(self, group):
        filt = {
            "filter":group
        }
        return filt

    def build_not_filter_group(self, group):
        filt = {
            "must_not":group
        }
        return filt

