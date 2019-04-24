import os
from glob import glob
from collections import OrderedDict
import pandas as pd
from ast import literal_eval

from twiddlepy.exceptions import LocationNotExist, SourceDataError
from twiddlepy.utils import logger, file_age_in_seconds

from .ds_base import DsBase

'''
    Class for file based data sources, direct sub class 
    include DsFileCsv and DsFileExcel
'''
class DsFileBase(DsBase):

    '''
        Class function to add a filename column to a dataframe

        Params:
            df: dataframe
            path: path of the file

        Returns:
            dataframe containing the filename column
    '''
    @classmethod
    def add_filename_to_df(cls, df, path):
        mdf = df.copy()
        mdf['filename'] = os.path.basename(path)
        return mdf


    def __init__(self, ds_type, config):
        super().__init__(ds_type, config)
        self.ds_unit = 'file'
        self.file_pattern = '*'

        ds_config = config[self.ds_config_section]
        self.source_location = ds_config['SourceLocation']
        self.archive_location = ds_config['ArchiveLocation']
        self.fail_location = ds_config['FailLocation']
        if ds_config['FilePattern'] != '':
            self.file_pattern = ds_config['FilePattern']


    '''
        Function that moves a file to archive/fail location (after it has been processed)

        Params:
            filepath: file to archive
            done: if the file has been successfully processed.

        Returns:
            None
    '''
    def archive_data(self, filepath, done=True):
        if done:
            archive_location = self.archive_location
            archive_label = 'Archive'
        else:
            archive_location = self.fail_location
            archive_label = 'Fail'
            
        if archive_location is None:
            logger.error('{} Location "{}" is not specified'.format(archive_label, archive_location))
            raise LocationNotExist('{} Location "{}" is not specified'.format(archive_label, archive_location))
            
        source_path = os.path.join(self.source_location, filepath)
        archive_path = os.path.join(archive_location, filepath)
        archive_base = os.path.dirname(archive_path)
        
        if not os.path.exists(archive_base):
            logger.info('{} directory {} does not exist, creating it.'.format(archive_label, archive_base))
            os.makedirs(archive_base)

        logger.info('Archiving file to {}'.format(archive_path))
        if os.path.exists(source_path):
            os.rename(source_path, archive_path)


    '''
        Function that traverse a directory and returns a list of files matching given file pattern.

        Params:
            dirpath: directory to traverse
            file_age: minimum age (in seconds) of the files

        Returns:
            list of files matching file_pattern and contained in file_subset (if not None)
    '''
    def get_data_files(self, file_age=60.00):
        dirpath = self.source_location
        if not dirpath.endswith('/'):
            dirpath += '/'

        dfiles = [f for d in os.walk(dirpath) for f in glob(os.path.join(d[0], self.file_pattern)) if file_age_in_seconds(f)>file_age]

        return [f.replace(dirpath, '') for f in dfiles]

    '''
        Function that is essentially alias of get_data_files()
    '''
    def get_data_units(self):
        return self.get_data_files()


    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return ''
         
'''
    Class for reading csv data sources and convert into pandas dataframe.
'''
class DsFileCsv(DsFileBase):
    def __init__(self, config):
        super().__init__('file.csv', config)
        ds_config = config[self.ds_config_section]
        # self.column_separator = literal_eval(ds_config['ColumnSeparator'])
        self.column_separator = literal_eval(ds_config['ColumnSeparator'])
        self.decimal_point = ds_config['DecimalPoint']
    
    '''
        Function that returns a dataframe generator object for the files in 
        the specified data source location 

        Params:
            datafile -- Path to the CSV to read
            dtype -- dictionary specifying column data types 
            sep -- column separator
            decimal -- decimal point character
    '''
    def read_data_to_df(self, datafile, dtype=None):
        try:
            logger.info('Reading file {}'.format(datafile))
            dfile = os.path.join(self.source_location, datafile)
            df = pd.read_csv(dfile, dtype=dtype, sep=self.column_separator, decimal=self.decimal_point, quotechar="'")
            df = DsFileBase.add_filename_to_df(df, datafile)
            return df
        except Exception as e:
            logger.error('Failed to read file "{}" due to error {}'.format(datafile, e))
            raise SourceDataError('Failed to read file "{}"'.format(datafile))
    
         
    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'CSV file'

class DsFileJson(DsFileBase):
    def __init__(self, config):
        super().__init__('file.json', config)

        ds_config = config[self.ds_config_section]

    '''
        Function that returns a dataframe generator object for the files in 
        the specified data source location 

        Params:
            datafile -- Path to the JSON to read
            dtype -- dictionary specifying column data types 
            sep -- column separator
            decimal -- decimal point character
    '''
    def read_data_to_df(self, datafile, dtype=None):
        try:
            logger.info('Reading file {}'.format(datafile))
            dfile = os.path.join(self.source_location, datafile)
            df = pd.read_json(dfile, dtype=dtype)
            df = DsFileBase.add_filename_to_df(df, datafile)
            return df
        except Exception as e:
            logger.error('Failed to read file "{}" due to error {}'.format(datafile, e))
            raise SourceDataError('Failed to read file "{}"'.format(datafile))
         
'''
    Class for reading MS Excel data sources and convert into pandas dataframe.
'''
class DsFileExcel(DsFileBase):
    def __init__(self, config):
        super().__init__('file.excel', config)

        ds_config = config[self.ds_config_section]
        if ds_config['Sheets'] == '':
            self.sheets = None
        else:
            self.sheets = ds_config['Sheets'].split()
            #
            # use string so that pd.read_excel returns a dataframe
            if len(self.sheets) == 1:
                self.sheets = self.sheets[0]

    
    '''
        Function that returns a dataframe or dict of dataframes
        for a specified file 

        Params:
            datafile: Path to MS the Excel to read
            dtype: dictionary specifying column data types 
    '''
    def read_data_to_df(self, datafile, dtype=None):
        try:
            logger.info('Reading file {}'.format(datafile))
            dfile = os.path.join(self.source_location, datafile)
            dfs = pd.read_excel(dfile, dtype=dtype, sheet_name=self.sheets)
            if isinstance(dfs, OrderedDict):
                mdfs = {}
                for _, (name, sheet_df) in enumerate(dfs.items()):
                    sheet_df = DsFileBase.add_filename_to_df(sheet_df, datafile)
                    sheet_df['sheetname'] = name
                    mdfs[name] = sheet_df
            else:
                mdfs = DsFileBase.add_filename_to_df(dfs, datafile)
            return dfs
        except Exception as e:
            logger.error('Failed to read file "{}" due to error {}'.format(datafile, e))
            raise SourceDataError('Failed to read file "{}"'.format(datafile))

    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'Excel file' 
         
'''
    Class for reading custom data sources and convert into pandas dataframe.
'''
class DsFileCustom(DsFileBase):
    def __init__(self, config):
        super().__init__('file.custom', config)

        ds_config = config[self.ds_config_section]
        if ds_config['FileParser'] == '':
            raise ValueError('FileParser must be specified for CustomDataSource')

        file_parser_name = ds_config['FileParser']

        try:
            import local_functions
            self.file_parser = getattr(local_functions, file_parser_name, None)
        except ImportError as e:
            raise ValueError('Failed to import local functions due to error "{}"'.format(e))
        
        if self.file_parser is None:
            raise ValueError('FileParser "{}" not found in local_functions.py'.format(file_parser_name))

    
    '''
        Function that returns a dataframe generator object for the files in 
        the specified data source location 

        Params:
            datafile -- Path to MS the Excel to read
            dtype -- dictionary specifying column data types 
    '''
    def read_data_to_df(self, datafile, dtype=None):
        try:
            logger.info('Reading file {}'.format(datafile))
            dfile = os.path.join(self.source_location, datafile)
            df = self.file_parser(dfile, dtype=dtype)
            df = DsFileBase.add_filename_to_df(df, datafile)
            return df
        except Exception as e:
            logger.error('Failed to read file "{}" due to error {}'.format(datafile, e))
            raise SourceDataError('Failed to read file "{}"'.format(datafile))

    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return 'Custom file'
         
