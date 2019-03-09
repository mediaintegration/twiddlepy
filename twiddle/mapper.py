import sys
import math
import pandas as pd
from pandas.errors import EmptyDataError
import json

from dateutil.parser import parse
from dateutil.tz import gettz

from pandas_schema import Column, Schema
from pandas_schema.validation import LeadingWhitespaceValidation, \
TrailingWhitespaceValidation, CanConvertValidation, MatchesPatternValidation, \
InRangeValidation, InListValidation

from .exceptions import MapperError

from .utils import logger, df_copy

'''
    Class to describe source data field names and types and 
    optionally to translate them to new names and types.

    Mapper is described in a CSV input file. 
'''
class Mapper:

    '''
        Class method to parse configuration inputs for mapper.

        Params:
            mapper_config: configuration parameters for section 'Mapper'

        Returns:
            dictionary containing the parsed configs.
    '''
    @classmethod
    def parse_config(cls, mapper_config):
        config = {}
        config['file'] = mapper_config['File']
        if config['file'] == '':
            raise MapperError('Mapper file "[Mapper][File]" must be specified')

        if mapper_config['ColumnType'] != '':
            config['column_type'] = json.loads(mapper_config['ColumnType'])

        if mapper_config['DataSets'] != '':
            config['datasets'] = mapper_config['DataSets'].split()

        return config

    '''
        Class method to filter the mapper for the given datasets.

        Params:
            mdf: original mapper dataframe from CSV file read
            datasets: list of datasets against which mapper is extracted

        Returns:
            mapper (dataframe) for the specified datasets
            (if the specified datasets is not a list, original mdf is returned)
    '''
    @classmethod
    def filter_mapper(cls, df, datasets):
        mdf = df.copy()
        if isinstance(datasets, list):
            return mdf[mdf['dataset'].isin(datasets)]
        elif isinstance(datasets, str):
            return mdf[mdf['dataset'] == datasets]
        else:
            return mdf


    '''
        Function to initialise a Mapper object. After an object is
        instantiated, it contains the mapper built from CSV mapper file.

        Params: 
            mapper_config: config parameters for config section 'Mapper'
    '''
    def __init__(self, mapper_config):
        self.config = Mapper.parse_config(mapper_config)

        try:
            mdf = pd.read_csv(self.config['file'], dtype=self.config['column_type'])
            mdf = mdf[mdf.ignore.str.lower()!='y']

            self.mapper_df = Mapper.filter_mapper(mdf, self.config.get('datasets', None))

            self.validation_schema = None
            self.validation_schema_for_dataset = {}
        except EmptyDataError as e:
            logger.error('No rows defined in mapper, aborting')
            raise e


    '''
        Function to return a copy of mapper dataframe

        Params:
            dataset: dataset for which mapper_df is extracted and returned

        Returns:
            mapper dataframe for the given dataset
    '''
    def get_mapper(self, dataset=None):
        if dataset is None:
            return df_copy(self.mapper_df)
        else:
            return Mapper.filter_mapper(self.mapper_df, dataset)

    '''
        Function to return source field type keyed on source field name.
    '''
    def get_source_data_types(self, df):
        return self.get_column_mapping(df, 'source_field_name', 'source_field_type')
    
    '''
        Function to return repository field type keyed on repository field name.
    '''
    def get_repository_data_types(self, df):
        return self.get_column_mapping(df, 'repository_field_name', 'repository_field_type')
    
    '''
        Function to return a dict of source_field_name to repository_field_name
    '''
    def get_source_to_repository_column_mapping(self, df):
        return self.get_column_mapping(df, 'source_field_name', 'repository_field_name')
    
    '''
        Function to return a dict mapping from column from_col to column to_col
        
        Params:
            df: mapper dataframe on which to extrat the mapping

        Returns:
            dict of columns mapping from from_col to to_col
    '''
    def get_column_mapping(self, df, from_col, to_col):
        mdf = df.copy()
        subf = mdf[mdf[from_col].notnull() & mdf[to_col].notnull()]
        mapping = zip(subf[from_col], subf[to_col])
        return dict(mapping)

    

    '''
        Function to return a validation schema for mapper dataframe dataset

        Params:
            dataset: dataset for which validataion_schema is returned

        Returns:
            mapper validation_schema for the given dataset
    '''
    def get_validation_schema(self, dataset=None):
        if dataset is None:
            if self.validation_schema is None:
                self.validation_schema = self.compile_source_data_validation_schema(dataset)
            return self.validation_schema
        else:
            if dataset not in self.validation_schema_for_dataset:
                test_df = Mapper.filter_mapper(self.mapper_df, dataset)
                self.validation_schema_for_dataset[dataset] = self.compile_source_data_validation_schema(dataset)
            return self.validation_schema_for_dataset[dataset]


    '''
        Function to validate a data dataframe with the schema

        Params:
            ddf: data dataframe
            schema: validation schema
            field_names: source field names to validate

        Returns:
            valid data dataframe with error rows removed and list of errors

    '''
    def validate_dataframe(self, ddf, schema, field_names=None):
        if not schema or field_names is None:
            logger.warn('No validation on the source data')
            return ddf, []

        if field_names:
            subf = ddf[field_names].copy()
        else:
            subf = ddf

        errors = schema.validate(subf)
        for error in errors:
            logger.warning(error)

        error_idx = list(set([error.row for error in errors]))
        if error_idx:
            index_start = ddf.index[0]
            idx_2_remove = [idx - index_start for idx in error_idx]
            valid_df = ddf.drop(ddf.index[idx_2_remove])
        
            return valid_df, error_idx
        return ddf, []


    '''
        Function to compile source data validation schema
        Params:
            dataset: dataset for which the validation schema is to be compiled.

        Returns:
            tuple of schema and list of source filed names to validate
    '''
    def compile_source_data_validation_schema(self, dataset):
        field_names = []
        field_schemas = []
    
        if dataset is not None:
            sub_map = Mapper.filter_mapper(self.mapper_df, dataset)
        else:
            sub_map = self.mapper_df

        sub_map = sub_map[sub_map['allow_missing'].str.lower() != 'y'].dropna(subset=['source_field_name','source_field_type'])

        for idx, field in sub_map.iterrows():
        
            field_validator = self.compile_field_validator(field)
            if field_validator:
                field_names.append(field['source_field_name'])

                field_schemas.append(Column(field['source_field_name'], field_validator))

        if not field_schemas:
            return None, []
    
        schema = Schema(field_schemas)
        return schema, field_names


    '''
        Function to compile validator for a field
        Params:
            field: a row in the mapper dataframe, ie a field description.

        Returns:
            field validator
    '''
    def compile_field_validator(self, field):
        field_validator = []

        if field['source_field_type'].lower() == 'int':
            field_validator.append(CanConvertValidation(int))
        elif field['source_field_type'].lower() == 'float':
            field_validator.append(CanConvertValidation(float))

        if pd.notnull(field['min']) and pd.notnull(field['max']):
            field_validator.append(InRangeValidation(field['min'], field['max']))
        elif pd.notnull(field['min']):
            field_validator.append(InRangeValidation(field['min'], math.inf))
        elif pd.notnull(field['max']):
            field_validator.append(InRangeValidation(-math.inf, field['max']))

        return field_validator



    '''
        Function to convert data dataframe df string columns specified as timestamp in
            the mapper dataframe mdf to the timestamp type.
    
        Params:
            ddf: data dataframe
            tz: time zone

        Returns:
            dataframe with timestamp columns converted to timestamp type
    '''
    def convert_datetime_column(self, ddf, tz=None):
        def datetime_parser(x, tz):
            if x:
                return parse(x, tzinfos=tz)
            return ''
    
        if tz is not None:
            tz = {k:gettz(v) for k, v in tz.items()}
        ts_cols = list(self.mapper_df[self.mapper_df.source_field_type=='timestamp']['source_field_name'])
        
        df = ddf.copy()

        # why does this not work
        #df[ts_cols] = df[ts_cols].apply(lambda x : datetime_parser(x, tz))

        for c in ts_cols:
            df[c] = df[c].apply(lambda x : datetime_parser(x, tz))
        return df


