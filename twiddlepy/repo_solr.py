import os, copy, json
import pandas as pd
from .connectors.pysolr import Solr, SolrCloud, ZooKeeper
from .exceptions import FieldTypeNotFound
from .utils import logger

class RepositorySolr:
    def __init__(self, solr_config):
        self.solr_config = solr_config

        if solr_config['ChunkSize'] == '':
            self.chunksize = 500
        else:
            self.chunksize = int(solr_config['ChunkSize'])

        solr_type_file = os.path.abspath(os.path.join(os.path.realpath(os.path.realpath(__file__)), '../data', 'solr_fieldtype_defaults.csv'))
        self.fieldtypes = self.load_fieldtypes(solr_type_file)

        if solr_config['BuildSchema'].lower() == 'false':
            self.should_build_schema = False
        else:
            self.should_build_schema = True

        if solr_config['StrictSchema'].lower() == 'true':
            self.strict_schema = True
        else:
            self.strict_schema = False

        if solr_config['RemoveZeroValues'].lower() == 'true':
            self.remove_zeros = True
        else:
            self.remove_zeros = False

        if solr_config['UserTypeFile'] != '':
            user_type_file = solr_config['UserTypeFile']
            self.fieldtypes.update(self.load_fieldtypes(user_type_file))

        solrCollection = solr_config['SolrCollection']
        solrUsername = solr_config['SolrUsername']
        solrPassword = solr_config['SolrPassword']
        solrSslVerify = solr_config['SolrSslVerify']

        if solr_config['ExtraFields'] != '':
            self.extra_fields = json.loads(solr_config['ExtraFields'])
        else:
            self.extra_fields = None

        if solrSslVerify.lower() == 'true':
            solrSslVerify = True
        elif solrSslVerify == '' or solrSslVerify.lower() == 'false':
            solrSslVerify = False

        if solr_config['SolrUrl'] != '':
            solrUrl = solr_config['SolrUrl']
            self.solr = Solr(solrUrl + '/' + solrCollection, timeout=60, 
                        auth=(solrUsername, solrPassword), 
                        verify=solrSslVerify)
        else:
            zkHost = solr_config['ZkHost']
            zkSolrNode = solr_config['ZkSolrNode']
            zkUsername = solr_config['ZkUsername']
            zkPassword = solr_config['ZkPassword']

            zookeeper = ZooKeeper(zkHost)
            self.solr = SolrCloud(zookeeper, solrCollection, timeout=10, 
                        auth=(solrUsername, solrPassword), 
                        verify=solrSslVerify)


    '''
        Function to close the Solr connection and drain the pool
    '''
    def close(self):
        if self.solr and self.solr.get_session():
            self.solr.get_session().close()

    '''
        Function to load Solr type definition file

        Params:
            type_file: Solr type definition file in csv format
            type_name_column: the fieldtype name column in the csv file

        Returns:
            dict containing Solr field type definition keyed on the type name
    '''
    def load_fieldtypes(self, type_filename, type_name_column='type_name'):
        stdf = pd.read_csv(type_filename)
        if type_name_column is not None:
            stdf = stdf.set_index(type_name_column)
        return stdf.T.to_dict()


    '''
        Function to commit a Pandas dataframe to Solr.
        Params:
            df: dataframe to commit to Solr
            remove_nan: if to remove rows containing Nan
            commit: if to apply Solr commit after update
    '''
    def commit_df(self, df, remove_nan=True, commit=True):
        if remove_nan and self.remove_zeros:
            self.solr.add([r[pd.notnull(r) & r!=0].to_dict() for _, r in df.iterrows()], commit=commit)
        elif remove_nan:
            self.solr.add([r[pd.notnull(r)].to_dict() for _, r in df.iterrows()], commit=commit)
        elif self.remove_zeros:
            self.solr.add([r[r!=0].to_dict() for _, r in df.iterrows()], commit=commit)
        else:
            self.solr.add([r.to_dict() for _, r in df.iterrows()], commit=commit)


    '''
        Function to commit a Pandas dataframe in chunks of size chunksize to Solr.
        Params:
            df: dataframe to commit to Solr
            remove_nan: if to remove rows containing Nan
            commit: if to apply Solr commit after update
    '''
    def commit_df_in_chunks(self, df, remove_nan=True, commit=True):
        sz_df = len(df)

        if sz_df == 0:
            return

        logger.info('Committing {} records to Solr'.format(len(df)))

        for pos in range(0, sz_df, self.chunksize):
            pos_end = pos + self.chunksize
            if pos_end > len(df):
                pos_end = len(df)
            self.commit_df(df[pos:pos_end], remove_nan=remove_nan, commit=commit)
            logger.info('{} records of {} committed to Solr'.format(str(pos_end).rjust(10), sz_df))

    '''
        Function to interface solrconn search

        Params:
            q: query
            search_handler: handler to perform search
            kwargs: other Solr params

        Returns:
            solrconn search results
    '''
    def search(self, q, search_handler=None, **kwargs):
        return self.solr.search(q, search_handler=search_handler, **kwargs)


    '''
        Function to interface solrconn delete

        Params:
            id: id of doc to delete
            q: query for docs to delete
            handler: handler to perform delete
            commit: if to commit post update

        Returns:
            solrconn update results
    '''
    def delete(self, id=None, q=None, commit=True, handler='update'):
        return self.solr.delete(id=id, q=q, commit=commit, handler=handler)


    '''
        Function to commit updates to Solr

        Params:
            kwargs: Solr params

        Returns:
            update results
    '''
    def commit(self, **kwargs):
        return self.solr.commit(**kwargs)

    '''
        Function to fetch schema field types

        Params:
            field_names: List of schema field names to fetch fields for

        Returns:
            list of schema field types
    '''
    def get_schema_field_types(self, field_names=None):
        schema_fields = self.solr.query_schema(handler='/schema/fields')['fields']
        if field_names is None:
            return schema_fields
        else:
            return [f for f in schema_fields if f['name'] and f['name'] in field_names]

    '''
        Function to add solr_fields to solr Schema

        Params:
            solr_fields: dict of field types keyed on field names
    '''
    def add_schema_fields(self, solr_fields):
        # get existing fields
        cf = self.get_schema_field_types()
        curr_fields = {f['name']:f for f in cf}
        curr_field_names = curr_fields.keys()
        for fname, ftype in solr_fields.items():
            type_def = self.fieldtypes.get(ftype, None)
            if type_def is not None:
                field_to_add = copy.deepcopy(type_def)
                field_to_add['name'] = fname
                if fname.lower() == 'id':
                    field_to_add['required'] = True
                if fname in curr_field_names:
                    cfield = curr_fields[fname]
                    update_field = False if cfield==field_to_add else True
                    if update_field:
                        if self.strict_schema:
                            raise ValueError('Solr field "{}" mismatched for strict schema\n    current: {} \n    new:     {}'.format(fname, str(cfield), str(field_to_add)))
                        
                        add_command = {'replace-field': field_to_add }

                        logger.info('    replace solr field {}'.format(fname))
                        self.solr.update_schema(payload=json.dumps(add_command))
                else:
                    add_command = {'add-field': field_to_add }

                    logger.info('    adding solr field "{}"'.format(fname))
                    self.solr.update_schema(payload=json.dumps(add_command))
            else:
                logger.error('Solr field type "{}" not found in the field definition'.format(ftype))
                raise FieldTypeNotFound('Solr field type "{}" not found in the field definition'.format(ftype))
                

