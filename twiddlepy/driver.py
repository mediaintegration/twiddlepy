#!/usr/bin/env python3

import os, sys, time
from collections import OrderedDict

from .config import config
from .ds_manager import DatasourceManager
from .repo_manager import RepositoryManager
from .mapper import Mapper

from .utils import logger
from .exceptions import TwiddleException, ExectionError

try:
    sys.path.append(os.getcwd())
    import local_functions
    if config['Processing']['TransformationProc'] != '':
        trans_func_name = config['Processing']['TransformationProc']
        print(f'Transformation function name: {trans_func_name}')
        transformation_function = getattr(local_functions, trans_func_name, None)
        xsheet_proc_name = config['Processing']['ExcelCrossSheetProc']
        excel_cross_sheet_proc = getattr(local_functions, xsheet_proc_name, None)

        source_header_tidier_name = config['Processing']['SourceColumnHeaderTidier']
        source_header_tidier_func = getattr(local_functions, source_header_tidier_name, None)
        if source_header_tidier_func is None:
            from twiddlepy import utils
            source_header_tidier_func = getattr(utils, source_header_tidier_name, None)


    if config['Processing']['PreMapTransformationProc'] != '':
        pre_map_func_name = config['Processing']['PreMapTransformationProc']
        print(f'Pre-map Transformation Function name: {pre_map_func_name}')
        premap_transformation_function = getattr(local_functions, pre_map_func_name, None)

except ImportError as e:
    logger.warning('Failed to import local_functions due to {}'.format(e))
    transformation_function = None
    excel_cross_sheet_proc = None
    source_header_tidier_func = None


class TwiddleDriver:
    def __init__(self, config):
        self.config = config

        self.mapper = Mapper(config['Mapper'])

        self.datasource = DatasourceManager(config).get_datasource()

        self.repository = RepositoryManager(config).get_repository()
        self.should_build_repository_schema = self.repository.should_build_schema

        if config['Processing']['WaitForData'] == '' or config['Processing']['WaitForData'].lower() == 'true':
            self.wait_for_data = True
        else:
            self.wait_for_data = False


    def build_repository_schema(self):
        logger.info('Building repository schema')
        repository_field_type = self.mapper.get_repository_data_types(self.mapper.get_mapper())
        repository_field_type.update(self.repository.extra_fields)
        self.repository.add_schema_fields(repository_field_type)

        
    def process_data(self):
        if self.should_build_repository_schema:
            self.build_repository_schema()
        else:
            logger.info('Skipping building repository schema')
            
        source_field_type = self.mapper.get_source_data_types(self.mapper.get_mapper())
        source_to_repo_mapping = self.mapper.get_source_to_repository_column_mapping(self.mapper.get_mapper())

        waiting = False
        while True:
            data_units = self.datasource.get_data_units()

            for dunit in data_units:
                try:
                    df = self.datasource.read_data_to_df(dunit, dtype='str')
                    if premap_transformation_function is not None:
                        try:
                            df = premap_transformation_function(df)
                        except Exception as e:
                            logger.error('Failed to execute transformation function "{}" due to error {}'.format(premap_transformation_function.__name__, e))
                            raise ExectionError('Failed to execute metadata processor "{}"'.format(premap_transformation_function.__name__))

                    df = df.astype(source_field_type)

                    if len(df) > 0:
                        waiting = False
                        logger.info('Processing {} "{}"...'.format(self.datasource.get_label(), dunit))

                    if not isinstance(df, OrderedDict):
                        qa_schema, qa_fields = self.mapper.get_validation_schema()
                        if transformation_function is not None:
                            df = self.process_dataframe(df, qa_schema, qa_fields, source_to_repo_mapping, transformation_function)
                        else:
                            df = self.process_dataframe(df, qa_schema, qa_fields, source_to_repo_mapping)
                        self.repository.commit_df_in_chunks(df)
                    else:
                        dfs = {}
                        for _, (sheet_name, sheet_df) in enumerate(df.items()):
                            qa_schema, qa_fields = self.mapper.get_validation_schema(dataset=sheet_name)
                            if isinstance(transformation_function, dict) and sheet_name in transformation_function:
                                trans = transformation_function[sheet_name]
                            else:
                                trans = None
                            dfs[sheet_name] = self.process_dataframe(sheet_df, qa_schema, qa_fields, source_to_repo_mapping, transformation_function)
                
                        if excel_cross_sheet_proc is not None:
                            dfs = excel_cross_sheet_proc(dfs)

                        for name, df in dfs.items():
                            self.repository.commit_df_in_chunks(df)

                    self.datasource.archive_data(dunit)
                except TwiddleException as e:
                    self.datasource.archive_data(dunit, done=False)
                except Exception as e:
                    logger.error('Error processing file "{}", due to error "{}"'.format(dunit, e))
                    self.datasource.archive_data(dunit, done=False)
                
            
            if not self.wait_for_data:
                break
            
            if not waiting:
                logger.info('Waiting for more source data to process...')
                waiting = True
            time.sleep(10)

    def process_dataframe(self, df, qa_schema, qa_fields, source_to_repo_mapping, transformation_function=None):
        if len(df) == 0:
            return df

        self.mapper.validate_dataframe(df, qa_schema, qa_fields)

        if source_header_tidier_func is not None:
            df.columns = [source_header_tidier_func(col) for col in df.columns]

        df = self.mapper.convert_datetime_column(df)
        df = df.rename(columns=source_to_repo_mapping)
        
        if transformation_function is not None:
            try:
                df = transformation_function(df)
            except Exception as e:
                logger.error('Failed to execute transformation function "{}" due to error {}'.format(transformation_function.__name__, e))
                raise ExectionError('Failed to execute metadata processor "{}"'.format(transformation_function.__name__))
        return df


if __name__ == '__main__':
    driver = TwiddleDriver(config)
    driver.process_data()
