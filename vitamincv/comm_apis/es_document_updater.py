import os
from glob import iglob
from pprint import pprint
import json
import glog as log
import pkg_resources

from vitamincv.comm_apis.es_query_builder import QueryBuilder
from vitamincv.avro_api.avro_api import AvroAPI

from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions

class DocumentUpdater():
    def __init__(self, vpc_endpoint, index, doc_type, global_timeout=60):
        # self.es_general = es.ElasticSearch(**defaults)
        self.settings={
            'vpc_endpoint': vpc_endpoint,
            'index': index,
            'doc_type': doc_type,
            'global_timeout': global_timeout
        }
        print(self.settings)
        self.es = Elasticsearch(self.settings['vpc_endpoint'], timeout=global_timeout)
        self._uploaded_scripts = set()
        self._upload_scripts()
        self.doc_transform = self._doc_transformer_default

    def _doc_transformer_default(self, doc):
        return doc

    def _if_script_exists(self, script_name):
        try:
            self.es.get_script(id=script_name)
            return True
        except es_exceptions.NotFoundError:
            return False

    def _upload_scripts(self):
        #curl command to put in the scripts.
        log.info('Uploading scripts:')
        path_to_painless_scripts = pkg_resources.resource_filename('vitamincv', os.path.join('comm_apis', 'painless_scripts/'))
        scripts = iglob(path_to_painless_scripts+'/*.painless')
        for script in scripts:
            script_name = script.split('/')[-1].rsplit('.')[0]
            if self._if_script_exists(script_name):
                continue
            script_data = json.load(open(script))
            self.es.put_script(id=script_name, body=script_data)
        self._get_uploaded_scripts()

    def _get_uploaded_scripts(self):
        resp = self.es.cluster.state()
        self._uploaded_scripts = set(resp['metadata'].get('stored_scripts', {}).keys())

    def get_doc_id_from_url(self, url):
        # self.es.search(index='', doc_type='',body='', _source=False)
        qb = QueryBuilder()
        include = {
            "url": url
        }
        qb.set(include=include)
        query = qb.build()
        resp = self.es.search(index=self.settings['index'], doc_type=self.settings['doc_type'], body=query, _source=False)
        if resp.get('hits',{}).get('hits') == []:
            return None
        return resp.get('hits',{}).get('hits',[{'_id':None}])[0]['_id']

    def get_document_by_id(self, doc_id):
        return self.es.get(index=self.settings['index'], doc_type=self.settings['doc_type'], id=doc_id)

    def run_stored_update_script(self, doc_id, script_name, params):
        script = {
            "script":{
                "id": script_name,
                "params": params
            }
        }
        if script_name in self._uploaded_scripts:
            self.es.update(index=self.settings['index'], doc_type=self.settings['doc_type'], id=doc_id, body=script)

    def add_media_doc(self, doc):
        self.es.index(index=self.settings['index'], doc_type=self.settings['doc_type'], body=doc)

    def extract_min_max_points(self, contour):
        x0 = 1
        x1 = 0
        y0 = 1
        y1 = 1
        for pt in contour:
            x0 = min(x0, pt['x'])
            x1 = max(x1, pt['x'])
            y0 = min(y0, pt['y'])
            y1 = max(y1, pt['y'])
        return [x0, x1, y0, y1]

    def update_footprint(self, doc_id, footprints):
        if type(footprints) == type(dict()):
            if 'array' in footprints.keys():
                footprints = footprints['array']
        script_name = 'update_footprint'
        for footprint in footprints:
            params = {
                'entry': footprint
            }
            self.run_stored_update_script(doc_id,script_name,params)

    def update_property(self, doc_id, section_type, idxs, jdx, new_properties):
        if new_properties == None:
            return
        script_name = 'update_properties'
        for new_property in new_properties:
            params = {
                'type': section_type,
                'idxs': idxs,
                'jdx': jdx,
                'entry': new_property
            }
            print('UPDATE PARAMS to '+doc_id+': ')
            print(params)
            resp = self.run_stored_update_script(doc_id,script_name,params)

    def update_region(self, doc_id, section_type, idxs, new_region, current_region):
        if new_region == None:
            return
        if type(new_region) is dict:
            new_region_anns = new_region.get('array', [])
        else:
            new_region_anns = new_region

        if type(current_region) is dict:
            current_region_anns = current_region.get('array', [])
        else:
            current_region_anns = current_region
        script_name = 'update_region'
        for new_reg in new_region_anns:
            new_contour = new_reg['contour']
            new_contour = self.extract_min_max_points(new_contour)
            found_contour = False
            for jdx, current_reg in enumerate(current_region_anns):
                if not new_contour == self.extract_min_max_points(current_reg['contour']):
                    continue
                found_contour = True
                self.update_property(doc_id, section_type, idxs, jdx, new_reg['props'])
                if not(new_reg.get('sub_regions') is None):
                    for subregion in new_reg.get('sub_regions', {}).get('array',[]):
                        jdxs = idxs.copy()
                        jdxs.append(jdx)
                        self.update_region(doc_id, section_type, jdxs, subregion, current_region)
            if found_contour:
                continue
            print('Adding Region to: '+doc_id)
            params = {
                'type': section_type,
                'idxs': idxs,
                'entry': new_reg
            }
            self.run_stored_update_script(doc_id,script_name,params)

    def update_frames_annotation(self, doc_id, current_doc, new_doc):
        if new_doc is None:
            log.warning("new_doc is None.")
            return
        if type(new_doc) is dict:
            new_doc_anns = new_doc.get('array', [])
        else:
            new_doc_anns=new_doc

        if type(current_doc) is dict:
            current_doc_anns = current_doc.get('array', [])
        else:
            current_doc_anns = current_doc
        script_name = 'update_frames_annotation'
        for new_ann in new_doc_anns:
            t_to_update = float(new_ann['t'])
            found_t = False
            for idx, current_ann in enumerate(current_doc_anns):
                if not float(current_ann['t']) == t_to_update:
                    continue
                found_t = True
                self.update_region(doc_id, 'frames_annotation', [idx], new_ann['regions'], current_ann['regions'])
                break
            if found_t:
                continue
            params = {
                'entry': new_ann
            }
            self.run_stored_update_script(doc_id,script_name,params)

    def update_media_summary(self, doc_id, current_doc, new_doc):
        if type(new_doc) is dict:
            new_doc_anns = new_doc.get('array', [])
        else:
            new_doc_anns=new_doc
        if type(current_doc) is dict:
            current_doc_anns = current_doc.get('array', [])
        else:
            current_doc_anns = current_doc

        script_name = 'update_media_summary'
        for new_ann in new_doc_anns:
            t1_to_update = float(new_ann['t1'])
            t2_to_update = float(new_ann['t2'])
            found_t = False
            for idx, current_ann in enumerate(current_doc_anns):
                if not float(current_ann['t1']) == t1_to_update and not float(current_ann['t2']) == t2_to_update:
                    continue
                found_t = True
                print('Found Region')
                self.update_region(doc_id, 'media_summary', idx, new_ann['regions'], current_ann['regions'])
                break
            if found_t:
                continue
            params = {
                'entry': new_ann
            }
            self.run_stored_update_script(doc_id,script_name,params)

    def update_es_doc(self, new_doc, doc_id = None):
        new_doc = self.doc_transform(new_doc)
        try:
            if type(new_doc) == type(str()):
                new_doc = json.loads(new_doc)
            if 'annotationData' in new_doc.keys():
                new_doc = new_doc['annotationData']
            if 'task_id' in new_doc.keys():
                new_doc = self.HAM2avro(new_doc)
            new_doc = AvroAPI(new_doc)
        except ValueError:
            log.warning("Invalid format")
            return

        print('Searching for URL: '+new_doc.get_url())
        if not doc_id:
            doc_id = self.get_doc_id_from_url(new_doc.get_url())
        if not doc_id:
            print('Adding New Document')
            self.add_media_doc(json.dumps(new_doc.doc))
            return
        print('Updating Existing Document... doc_id: '+doc_id)
        full_doc = self.get_document_by_id(doc_id)
        # full_doc = full_doc.json()
        full_doc = AvroAPI(full_doc['_source'])
        footprint = new_doc.get_footprints()
        self.update_footprint(doc_id, footprint)
        self.update_frames_annotation(doc_id, full_doc.get_image_anns(), new_doc.get_image_anns())
        if new_doc.get_media_summaries():
            self.update_media_summary(doc_id, full_doc.get_media_summaries(), new_doc.get_media_summaries())
