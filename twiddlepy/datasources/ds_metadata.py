import os, sys
from glob import glob
import json
import copy
from datetime import datetime
from collections import OrderedDict
import pandas as pd

from twiddlepy.exceptions import SourceDataError, ExectionError
from twiddlepy.utils import logger, file_age_in_seconds

from kazoo.client import KazooClient

from .ds_file import *
from .ds_sql import *

from .ds_base import DsBase


try:
    sys.path.append(os.getcwd())
    import local_functions
    has_local_functions = True
except ImportError as e:
    has_local_functions = False


'''
    Class for metadata based data sources, direct sub class 
    include DsMetadataFile and DsMetadataZookeeper
'''
class DsMetadataBase(DsBase):

    '''
        Metadata is json of the format (use either filename or tablename):
            {
            "id": "{id}",                           # id for the metadata
            "type": {type}",                        # type of the data source
            "source_name": "{filename|tablename}",  # file or table name the metadata is for
            "status": "[READY|PROCESSING|COMPLETE|FAIL]", 
            "timestamp": "{timestamp of last status update}",
            "other_data": {"key": "{value}"}        # additional metadata
            }
    '''

    def __init__(self, ds_type, config):
        super().__init__(ds_type, config)
        self.ds_unit = 'metadata'

        self.source_metadata = {}
        self.datasources = {}

        self.metadata_proc_name = config[self.ds_config_section]['MetadataProcessor']
        if self.metadata_proc_name != '' and has_local_functions:
            self.metadata_proc = getattr(local_functions, self.metadata_proc_name, None)
        else:
            self.metadata_proc = None

    '''
        Function that returns list of metadata ids for
        status being 'READY'
    '''
    def get_metadata_id_for_status(self, status='READY'):
        return [md['id'] for _, md in self.source_metadata.items() if md['status'].upper() == status.upper()]


    '''
        Function to update metadata status

        Params:
            mda_id: id for metadata
    '''
    def update_status(self, mda_id, status):
        if mda_id not in self.source_metadata:
            raise SourceDataError('Metadata not found for id "{}"'.format(mda_id))

        self.source_metadata[mda_id]['status'] = status

    
    '''
        Function to update metadata in the store (fs or zk)

        Params:
            mda_id: id for metadata
    '''
    def update_metadata(self, mda_id):
        pass

    
    '''
        Function that returns a dataframe generator object for the files in 
        the specified data source location 

        Params:
            src_md_id: id for soure metadata
            dtype: dictionary specifying column data types 
    '''
    def read_data_to_df(self, src_md_id, dtype=None):
        logger.info('Reading metadata {}'.format(src_md_id))
        if src_md_id not in self.source_metadata:
            logger.error('Metadata not found for id "{}"'.format(src_md_id))
            raise SourceDataError('Metadata not found for id "{}"'.format(src_md_id))

        src_metadata = self.source_metadata[src_md_id]
        if src_metadata['status'] != 'READY':
            return

        # Normalise the type first, in case of mistype, e.g. mixed cases.
        ds_config_section = 'Ds' + src_metadata['type'].lower().title().replace('.', '')

        if self.datasources.get(ds_config_section, None) is None:
            ds_cls = getattr(sys.modules[__name__], ds_config_section, None)
            if ds_cls is None:
                logger.error('Unrecognised datasource type "{}".'.format(src_metadata['type']))
                raise SourceDataError('Unrecognised datasource type "{}".'.format(src_metadata['type']))
            self.datasources[ds_config_section] = ds_cls(self.config)

        source_name = src_metadata.get('source_name', None)
        if source_name is None:
            logger.error('Metadata "{}" is does not contain a source_name'.format(src_metadata['id']))
            raise SourceDataError('Metadata "{}" is does not contain a source_name'.format(src_metadata['id']))
        self.update_metadata_status(src_md_id, 'PROCESSING')
        
        df = self.datasources[ds_config_section].read_data_to_df(source_name, dtype=dtype)

        if self.metadata_proc is not None:
            try:
                df = self.metadata_proc(df, src_metadata)
            except Exception as e:
                logger.error('Failed to execute metadata processor "{}" due to error {}'.format(self.metadata_proc_name, e))
                raise ExectionError('Failed to execute metadata processor "{}"'.format(self.metadata_proc_name))

        return df
    

    '''
        Function that archives the file specified by the job metadata_id

        Params:
            mda_id: metadata id
    '''
    def archive_data(self, mda_id, done=True):
        self.update_metadata_status(mda_id, 'COMPLETE' if done else 'FAIL')

        src_metadata = self.source_metadata[mda_id]
        # Normalise the type first, in case of mistype, e.g. mixed cases.
        ds_config_section = 'Ds' + src_metadata['type'].lower().title().replace('.', '')
        ds = self.datasources[ds_config_section]
        ds.archive_data(src_metadata['source_name'], done=done)

    
    '''
        Function to update metadata in the store (fs or zk)

        Params:
            mda_id: id for metadata
    '''
    def update_metadata_status(self, mda_id, status):
        src_metadata = self.source_metadata[mda_id]
        src_metadata['status'] = status
        src_metadata['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        src_mda = copy.deepcopy(self.source_metadata[mda_id])

        path = src_mda['__path']
        del src_mda['__path']

        self.save_metadata(path, src_mda)

   
    def save_metadata(self, path, mda):
        pass



'''
    Class for processing file based metadata
'''
class DsMetadataFile(DsMetadataBase):
    def __init__(self, config):
        super().__init__('metadata.file', config)
        self.metadata_location = config['DsMetadataFile']['MetadataLocation']
        self.file_pattern = config['DsMetadataFile']['FilePattern']


    '''
        Function that traverse a directory and returns a list of metadata files.

        Params:

        Returns:
            list of files matching self.file_pattern 
    '''
    def get_metadata_files(self):
        dirpath = self.metadata_location
        return [f for d in os.walk(dirpath) for f in glob(os.path.join(d[0], self.file_pattern))]


    '''
        Function that traverse a directory and returns a list of metadata.

        Params:

        Returns:
            dict of source metadata keyed on metadata id
    '''
    def get_source_metadata(self):

        mfiles = self.get_metadata_files()
        for mf in mfiles:
            with open(mf) as f:
                jdata = json.load(f)
                jdata['__path'] = mf
                self.source_metadata[jdata['id']] = jdata

        return self.source_metadata


    '''
        Function that is essentially alias of get_data_files()
    '''
    def get_data_units(self):
        self.get_source_metadata()
        
        return self.get_metadata_id_for_status(status='READY')


    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'Metadata file'


    def save_metadata(self, path, mda):
        with open(path, 'w') as out:
            json.dump(mda, out)


'''
    Class for processing Zookeeper based metadata
'''
class DsMetadataZookeeper(DsMetadataBase):
    def __init__(self, config):
        super().__init__('metadata.zookeeper', config)
        zkhosts = config['DsMetadataZookeeper']['ZkHost'].split('/', 1)
        username = config['DsMetadataZookeeper']['ZkUsername']
        password = config['DsMetadataZookeeper']['ZkPassword']

        zkServer = zkhosts[0]
        self.base_znode = '/'
        if len(zkhosts) == 2 and zkhosts[1]:
            self.base_znode = '/' + zkhosts[1]
        
        if username and password:
            auth = [('digest', '{}:{}'.format(username, password))]
        else:
            auth = None
        
        self.zookeeper = KazooClient(zkServer, auth_data=auth,
                command_retry={'max_tries': 5}, connection_retry={'max_tries': 5})

        self.zookeeper.start()
        self.file_pattern = config['DsMetadataZookeeper']['FilePattern']


    '''
        Function to return all children znodes given the parent znode

        Params:
            parent_znode: parent znode

        Returns:
            a list of childrn znodes
    '''
    def get_all_children_znodes(self, parent_znode):
        nodes = []
        children = self.zookeeper.get_children(parent_znode)
        for c in children:
            child_znode = parent_znode + '/' + c
            nodes.append(child_znode)
            nodes.extend(self.get_all_children_znodes(child_znode))

        return nodes


    '''
        Function that traverse znode and returns a list of metadata files.

        Params:

        Returns:
            list of metadata znodes
    '''
    def get_metadata_nodes(self):
        znodes = self.get_all_children_znodes(self.base_znode)

        return znodes


    '''
        Function to return the content of a znode

        Params:
            znode: znode

        Returns:
            content of the specified znode
    '''
    def get(self, znode):
        return self.zookeeper.get(znode)

    '''
        Function to save string msg to znode

        Params:
            znode: znode
            msg: string to save to znode
    '''
    def update(self, znode, msg=''):
        self.zookeeper.set(znode, msg.encode('utf-8'))



    '''
        Function that traverse a directory and returns a list of metadata.

        Params:

        Returns:
            dict of source metadata keyed on metadata id
    '''
    def get_source_metadata(self):

        mnodes = self.get_metadata_nodes()
        for mn in mnodes:
            node_data, node_stats = self.get(mn)
            jdata = json.loads(node_data)
            jdata['__path'] = mn
            self.source_metadata[jdata['id']] = jdata

        return self.source_metadata


    '''
        Function that is essentially alias of get_data_files()
    '''
    def get_data_units(self):
        self.get_source_metadata()
        
        return self.get_metadata_id_for_status(status='READY')


    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'Metadata znode'


    def save_metadata(self, znode, mda):
        self.update(znode, json.dumps(mda))
