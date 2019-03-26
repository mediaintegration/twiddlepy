import os
import pickle
from glob import glob
import pandas as pd
import re
from sqlalchemy import create_engine
import cx_Oracle

from twiddlepy.exceptions import SourceDataError
from twiddlepy.utils import logger, uppercase

from .ds_base import DsBase

'''
    Class for Sql based data sources, direct sub class 
    include DsDatabaseMysql and DsDatabaseOracle
'''
class DsDatabaseBase(DsBase):

    '''
        Class function to add a table column to a dataframe

        Params:
            df: dataframe
            tablename: table name

        Returns:
            dataframe containing the table name column
    '''
    @classmethod
    def add_table_to_df(cls, df, tablename):
        mdf = df.copy()
        mdf['tablename'] = tablename
        return mdf


    def __init__(self, ds_type, config):
        super().__init__(ds_type, config)
        self.ds_unit = 'table'

        ds_config = config[self.ds_config_section]

        self.table_pattern = ds_config['TablePattern']

        self.select_columns = ds_config['TableColumns'].split()

        self.db_engine = None

        if ds_type != 'database.sqlite':
            self.db_server = ds_config['DbServer']
            self.db_name = ds_config['DbName']
            self.db_username = ds_config['DbUsername']
            self.db_password = ds_config['DbPassword']

        self.watermark_column = ds_config['WatermarkColumn']
        self.watermark_store = ds_config['WatermarkStore']
        if ds_config['ResetWatermark'].lower() == 'true':
            self.reset_watermark = True
        else:
            self.reset_watermark = False

        self.watermarks = {}

        if self.watermark_store and not self.reset_watermark:
            try:
                if os.path.isfile(self.watermark_store) and os.path.getsize(self.watermark_store) > 0:      
                    with open(self.watermark_store, 'rb') as wmstore:
                        self.watermarks = pickle.load(wmstore)
            except Exception as e:
                logger.warning('Failed to read watermark store "{}"'.format(self.watermark_store))
                raise e


    '''
        Function that returns a dataframe for a table
        Params:
            tablename: name of the table
            dtype: dictionary specifying column data types 
    '''
    def read_data_to_df(self, tablename, dtype=None):
        logger.info('Reading table {}'.format(tablename))

        if self.select_columns:
            columns = self.select_columns[:]
            if self.watermark_column and self.watermark_column not in columns:
                columns.append(self.watermark_column)
            query = 'select {c} from {t}'.format(c=','.join(columns), t=tablename)
        else:
            query = 'select * from {t}'.format(t=tablename)

        if self.watermark_column:
            wm = self.watermarks.get(tablename, None)
                    
            if wm is not None:
                if pd.api.types.is_number(wm):
                    sql = "{q} where {c} > {w} order by {c} asc".format(q=query, c=self.watermark_column, w=wm)
                else:
                    sql = "{q} where {c} > '{w}' order by {c} asc".format(q=query, c=self.watermark_column, w=wm)
            else:
                sql = '{q} order by {c} asc'.format(q=query, c=self.watermark_column)
        else:
            sql = query

        try:
            df = self.run_query(sql)
            
            if len(df.index)>0:
                if self.watermark_column:
                    self.watermarks[tablename] = df.iloc[-1][self.watermark_column]

        except Exception as e:
            logger.error('Failed to read table "{}" due to error {}'.format(tablename, e))
            raise SourceDataError('Failed to read table "{}"'.format(tablename))

        return df
    
         
    '''
        Function that returns a dataframe for from a sql query
        Params:
            q: sql query
    '''
    def run_query(self, q):
        try:
            df =  pd.read_sql_query(q, self.db_engine)
            # convert db column headers to uppper case
            df.columns = [uppercase(col) for col in df.columns]
            return df
            
        except Exception as e:
            logger.warning('Failed to run sql query "{}"'.format(q))
            raise e


    '''
        Function that is essentially alias of get_table_list()
    '''
    def get_data_units(self):
        return self.get_table_list()

    '''
        Function that list of tables
    '''
    def get_table_list(self):
        return []


    '''
        Function placeholder for archiving data source

        Params:
            tablename: table to archive
            done: if the file has been successfully processed.

        Returns:
            None
    '''
    def archive_data(self, tablename, done=True):
        self.persist_watermarks()

    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return ''
         
    '''
        Function that persists watermarks to fs
    '''
    def persist_watermarks(self):
        if self.watermark_store:
            with open(self.watermark_store, 'wb') as wmstore:
                pickle.dump(self.watermarks, wmstore)

    
    
'''
    Class for reading MySQL data sources and convert into pandas dataframe.
'''
class DsDatabaseMysql(DsDatabaseBase):
    def __init__(self, config):
        super().__init__('database.mysql', config)

        if self.db_username is not None and self.db_password is not None:
            dburl = 'mysql+pymysql://{u}:{p}@{s}/{d}'.format(s=self.db_server, d=self.db_name, u=self.db_username, p=self.db_password)
        else:
            dburl = 'mysql+pymysql://{s}/{d}'.format(s=self.db_server, d=self.db_name)

        self.db_engine = create_engine(dburl)


    '''
        Function that list of tables based on the table pattern
    '''
    def get_table_list(self):
        q = '''
             SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "{d}"
            '''.format(d=self.db_name)

        df = self.run_query(q)
        if self.table_pattern:
            return [t for t in list(df['TABLE_NAME']) if re.match(self.table_pattern, t)]
        else:
            return list(df['TABLE_NAME'])
            
    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'MySQL table'
         
    
class DsDatabaseOracle(DsDatabaseBase):
    
    def __init__(self, config):
        super().__init__('database.oracle', config)

        if self.db_username is not None and self.db_password is not None:
            dburl = 'oracle+cx_oracle://{u}:{p}@{s}/{d}'.format(s=self.db_server, d=self.db_name, u=self.db_username, p=self.db_password)
        else:
            dburl = 'oracle+cx_oracle://{s}/{d}'.format(s=self.db_server, d=self.db_name)

        self.db_engine = create_engine(dburl)

    '''
        Function that list of tables based on the table pattern
    '''
    def get_table_list(self):
        q = '''
             SELECT TABLE_NAME FROM ALL_TABLES
            '''

        df = self.run_query(q)
        if self.table_pattern:
            return [t for t in list(df['TABLE_NAME']) if re.match(self.table_pattern, t)]
        else:
            return list(df['TABLE_NAME'])

    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'Oracle table'
         

         
class DsDatabaseMssql(DsDatabaseBase):
    
    def __init__(self, config):
        super().__init__('database.mssql', config)

        if self.db_username is not None and self.db_password is not None:
            dburl = 'mssql+pymssql://{u}:{p}@{s}/{d}'.format(s=self.db_server, d=self.db_name, u=self.db_username, p=self.db_password)
        else:
            dburl = 'msgsql+pymssql://{s}/{d}'.format(s=self.db_server, d=self.db_name)

        self.db_engine = create_engine(dburl)

    '''
        Function that list of tables based on the table pattern
    '''
    def get_table_list(self):
        q = '''
             SELECT TABLE_NAME FROM {d}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' order by TABLE_NAME
            '''.format(d=self.db_name)

        df = self.run_query(q)
        if self.table_pattern:
            return [t for t in list(df['TABLE_NAME']) if re.match(self.table_pattern, t)]
        else:
            return list(df['TABLE_NAME'])

    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'MsSQL table'
         

class DsDatabaseSqlite(DsDatabaseBase):
    
    def __init__(self, config):
        super().__init__('database.sqlite', config)

        self.db_path = config[self.ds_config_section]['DbPath']
        dburl = 'sqlite:///{d}'.format(d=self.db_path)

        self.db_engine = create_engine(dburl)

    '''
        Function that list of tables based on the table pattern
    '''
    def get_table_list(self):
        q = '''
             SELECT name FROM sqlite_master WHERE type='table'
            '''

        df = self.run_query(q)
        if self.table_pattern:
            return [t for t in list(df['NAME']) if re.match(self.table_pattern, t)]
        else:
            return list(df['NAME'])

    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'Sqlite table'
         

            
            
            
            
