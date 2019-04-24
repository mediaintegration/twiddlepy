import os, copy, json
import pandas as pd
import numpy as np
from .connectors.pysolr import Solr, SolrCloud, ZooKeeper
from .exceptions import FieldTypeNotFound
from .utils import logger

class RepositoryCsv:
    def __init__(self, csv_config):
        self.csv_config = csv_config
        self.write_header = True
        self.file_path = csv_config['FilePath']
        self.sep = csv_config['ColumnSeparator']
        self.decimal = csv_config['DecimalPoint']
        
        # Check whether we should append to the given
        # file or overwrite it
        if csv_config['Append'].lower() == 'false':
            self.write_type = 'w'
        else:
            self.write_type = 'a'

        self.should_build_schema = False

    '''
        Function to write a Pandas dataframe to CSV file.
        Params:
            df: dataframe to commit to CSV file
    '''
    def commit_df(self, df, remove_nan=True):
        if remove_nan:
            mdf = df.replace(np.nan, '', regex=True)
        else:
            mdf = df
        
        with open(self.file_path, self.write_type, encoding='utf-8') as file:
            mdf.to_csv(file, header=self.write_header, decimal=self.decimal, sep=self.sep, index=False)
        
        self.write_header = False

    
    def commit_df_in_chunks(self, df, remove_nan=True):
        return self.commit_df(df, remove_nan=remove_nan)
