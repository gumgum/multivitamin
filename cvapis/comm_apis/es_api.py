
#https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html
#http://elasticsearch-py.readthedocs.io/en/master/
#scroll: https://gist.github.com/drorata/146ce50807d16fd4a6aa
import json
import pprint
import time
import os
from glob import glob, iglob
import logging
import pkg_resources

import glog as log
from elasticsearch import Elasticsearch, helpers

from cvapis.comm_apis.comm_api import CommAPI
from cvapis.comm_apis.es_query_builder import QueryBuilder
from cvapis.comm_apis.es_document_updater import DocumentUpdater
from cvapis.comm_apis import config

from cvapis.comm_apis.work_handler import WorkerManager

import inspect

class ESAPI(CommAPI):
    """ API for accessing CV ElasticSearch database """
    def __init__(self, vpc_endpoint=config.VPC_ENDPOINT, index=config.ES_INDEX, doc_type="media_annotations",
                 mapping_file=pkg_resources.resource_filename('cvapis', os.path.join('comm_apis', 'es_mapping.json')), global_timeout=60):
        """ 
        Args:
            vpc_endpoint (str): Web address to ElasticSearch cluster.
            index (str): ElasticSearch index to search for documents in.
            doc_type (str): Document type in ElasticSearch index. 
                Defaults to 'media_annotations'.
            mapping_file (str): File that defines datatypes for fields in ElasticSearch cluster.
                Defaults to 'comm_apis/es_mapping.json' in the install directory.
        """
        super().__init__()
        log.debug("Initializing")
        self.vpc_endpoint=vpc_endpoint
        self.index=index
        self.doc_type=doc_type
        self.mapping_file = mapping_file
        self.global_timeout = global_timeout
        self._workers = []
        log.debug("Creating the client connecting to " + self.vpc_endpoint)
        self.es = Elasticsearch(self.vpc_endpoint, timeout=self.global_timeout)
        tracer = logging.getLogger('elasticsearch')
        tracer.setLevel(logging.CRITICAL) # or desired level

        self.scripts_dir = "/painless_scripts"

        self.qbuilder = QueryBuilder()
        self.doc_updater = DocumentUpdater(self.vpc_endpoint, self.index, self.doc_type)
        self._create_index()
        self.upload_scripts(force=False)

    def _if_script_exists(self, script_name):
        try:
            self.es.get_script(id=script_name)
            return True
        except es_exceptions.NotFoundError:
            return False

    def upload_scripts(self, force=True):
        source_file = inspect.getfile(self.__class__)
        source_dir = os.path.dirname(source_file)
        painless_scripts_path = pkg_resources.resource_filename('cvapis', os.path.join('comm_apis', 'painless_scripts/'))
        for path in iglob(painless_scripts_path+"/*.painless"):
            name = path.split('/')[-1].replace(".painless", "")
            if not force and self._if_script_exists(name):
                continue
            painless = json.load(open(path))
            self.es.put_script(name, painless)

    def set_document_transform(self, function):
        self.doc_updater.doc_transform = function

    def remove_document_transform(self):
        self.doc_updater.doc_transform = self._doc_transformer_default

    def _create_index(self):
        if not self.es.indices.exists(index=self.index) and not '*' in self.index:
            log.info("Creating index: " + self.index)
            mapping = json.dumps(json.load(open(self.mapping_file)))
            mapping = mapping.replace('{type}', self.doc_type)
            self.es.indices.create(index=self.index, body=mapping)

    def _delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])

    def json_data_gen(self, source):
        """Collects jsons from local path.

        Args:
            source (str): A path to a file or directory.
                If a directory is provided, it will search for all '.json' files under that directory.
                If the loaded JSON is an array, it will yield each element in the array iteratively.

        Yields:
            dict: A python dictionary representing a single JSON
        """
        if os.path.isfile(source):
            print('Opening Single File: '+source)
            try:
                jsons = json.load(open(source))
            except:
                log.warning("Unable to load JSON: {}".format(source))
                return []
            if type(jsons) == type(list()):
                for j in jsons:
                    yield j
            else:
                yield jsons
        if os.path.isdir(source):
            json_paths = glob(source+'/**/*.json', recursive=True)
            for path in json_paths:
                try:
                    j = json.load(open(path))
                except:
                    log.warning("Unable to load JSON: {}".format(path))
                    continue
                print('Opening Array File: '+path)
                if type(j) == type([]):
                    for _j in j:
                        yield _j
                else:
                    yield j

    def _remove_empty_fields(self,dictionary):
        bad_keys = []
        for key in dictionary.keys():
            if dictionary[key] in [None, ""]:
                bad_keys.append(key)
            if type(dictionary[key]) == type({}):
                dictionary[key] = self._remove_empty_fields(dictionary[key])
        print(bad_keys)
        for key in bad_keys:
            del dictionary[key]
        return dictionary

    def set(self,footprints=[],props=[],dates=[],size=1,timeout=u'5m'):
        log.debug("Setting ESAPI")

        inc = []
        for p in props:
            inc_local={
                "frames_annotation": {
                    #                   "t_max": None,
                    #                  "t_min": None,
                    "regions": {
                        #                    "contour": {
                        #                       "x_min": None,
                        #                       "y_min": None,
                        #                       "x_max": None,
                        #                       "y_max": None
                        #                   },
                        "props": {
                            #                            "confidence_min_max": None,
                            "company": "",
                            "property_type": "",
                            "confidence_min": None,
                            #                      "value_verbose": "",
                            "value": "",
                            #                     "module_id": "",
                            #                    "confidence_min_min": None,
                            "confidence_max": None,
                            #                   "fraction_min": None,
                            #                   "property_id": "",
                            "server": "",
                            #                   "fraction_max": None
                        }
                    }
                }
            }
            inc_local['frames_annotation']['regions']['props'].update(p)
            inc_local_pretty = pprint.pformat(inc_local)
            log.debug(inc_local_pretty)
            inc.append(inc_local)
        for f in footprints:
            inc_local={
                "codes": {
                    "ver": "",
                    "company": "",
                    "code": "",
                    "server": "",
                    "date_max": None,
                    "date_min": None
                }
            }
            inc_local["codes"].update(f)
            if len(dates) == 1:
                inc_local['codes'].update(dates)
            inc.append(inc_local)
            inc_local_pretty = pprint.pformat(inc_local)
            log.debug(inc_local_pretty)

        for idx, d in enumerate(dates):
            inc[idx]['codes'].update(d)

        for idx, i in enumerate(inc):
            inc[idx] = self._remove_empty_fields(i)

        #############
#        inc = []
        #
        # [
            # {
            #   "h_min": None,
            #   "h_max": None,
            #   "w_min": None,
            #   "w_max": None,
            #   "url": "",
            #   "source_url": "",
            #   "codes": {
            #     "ver": "",
            #     "company": "",
            #     "code": "",
            #     "server": "",
            #     "date_max": None,
            #     "date_min": None
            #   },
            #   "frames_annotation": {
            #     "t_max": None,
            #     "t_min": None,
            #     "regions": {
            #       "contour": {
            #         "x_min": None,
            #         "y_min": None,
            #         "x_max": None,
            #         "y_max": None
            #       },
            #       "props": {
            #         "confidence_min_max": None,
            #         "company": "",
            #         "property_type": "",
            #         "confidence_min": None,
            #         "value_verbose": "",
            #         "value": "",
            #         "module_id": "",
            #         "confidence_min_min": None,
            #         "confidence_max": None,
            #         "fraction_min": None,
            #         "property_id": "",
            #         "server": "",
            #         "fraction_max": None
            #       }
            #     }
            #   },
            #   "media_summary": {
            #     "t2_max": None,
            #     "t1_min": None,
            #     "t2_min": None,
            #     "regions": {
            #       "contour": {
            #         "x_min": None,
            #         "y_min": None,
            #         "x_max": None,
            #         "y_max": None
            #       },
            #       "props": {
            #         "confidence_min_max": None,
            #         "company": "",
            #         "property_type": "",
            #         "confidence_min": None,
            #         "value_verbose": "",
            #         "value": "",
            #         "module_id": "",
            #         "confidence_min_min": None,
            #         "confidence_max": None,
            #         "fraction_min": None,
            #         "property_id": "",
            #         "server": "",
            #         "fraction_max": None
            #       }
            #     },
            #     "t1_max": None
            #   }
            # }
        # ]
        #
        self.qbuilder.set(include=inc)
        self.query =  self.qbuilder.build()# ARRAY OR DICTIONARY SHOULD GO IN HERE
        #############
        query_pretty = pprint.pformat(self.query)
        log.info("Setting a scan with query: " + str(query_pretty))
        self._set_with_query(self.query, timeout, size)

    def _set_with_query(self, query,timeout=u'5m',size=1):
        self.query = query
        self.results=helpers.scan(self.es,index=self.index,doc_type=self.doc_type, query=self.query, scroll=timeout, raise_on_error=True, preserve_order=True, size=size, request_timeout=None, clear_scroll=True, scroll_kwargs=None)
        self.counter_read=0
        length=self.results.__sizeof__()
        log.info("self.results length: " + str(length))
        return

    def count(self):
        resp = self.es.count(index=self.index, doc_type=self.doc_type, body=self.query)
        count = resp.get('count', 0)
        return count

    def delete_doc(self, doc_id):
        resp = self.es.delete(index=self.index, doc_type=self.doc_type, id=doc_id)

    def delete_doc_by_url(self, url):
        query = {
            "query":{
                "match_phrase":{
                    "url": url
                }
            }
        }
        return self.es.delete_by_query(index=self.index, doc_type=self.doc_type, body=query)

    def get_doc_id_from_url(self, url):
        return self.doc_updater.get_doc_id_from_url(url)

    def pull_gen(self, n=1):
        while True:
            data = self.pull(n)
            if data:
                yield data
            else:
                break

    def pull(self,n=1):
        super().pull(n)
        data=[]
        log.info("Going through the items in the scan")
        while len(data)<n:
            try:
                item=next(self.results)
            except:
                log.info("No more items in the the generator")
                break
            data.append(item["_source"])
        log.info("len(data): " + str(len(data)))
        return data

    def push(self):
        pass
    def push_from_folder(self, data_source, num_workers=1, max_queue_size=100):
        # data_source can be a string representing a path to directory containing *.json files at any depth, a string representing a path to a file containing a json or an array of jsons, a list of python dictionaries, or a python dictionary

        # log.debug("push not implemented.")

        self.work_manager = WorkerManager(self.doc_updater.update_es_doc, n=num_workers, max_queue_size=max_queue_size)
        if type(data_source) == type(str()):
            for j in self.json_data_gen(data_source):
                self.work_manager.queue.put(j)
        if type(data_source) == type(list()):
            for j in data_source:
                self.work_manager.queue.put(j)
        if type(data_source) == type(dict()):
            self.work_manager.queue.put(data_source)
        self._kill_workers_on_queue_empty()
