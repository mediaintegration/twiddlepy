
'''
    Base class for all datasources
'''
class DsBase:

    def __init__(self, ds_type, config):
        self.ds_type = ds_type
        self.config = config
        self.ds_config_section = 'Ds' + ds_type.lower().title().replace('.', '')
        self.ds_unit = ''

    '''
        Function to archive data.

        Params:
            unit_path: filepath, tablename or metadata id
            done: is processing done

        Returns:
    '''
    def archive_data(self, unit_path, done=True):
        pass

    '''
        Function that returns either a filepath, a table or a metadata id
    '''
    def get_data_units(self):
        raise NotImplementedError('Function "get_data_units" has not been implemented')


    
    '''
        Function that returns a dataframe generator object for the specified data unit
        data unit being a file, a table or a metadata id

        Params:
            dataunit -- Path to the CSV to read
            dtype -- dictionary specifying column data types 
    '''
    def read_data_to_df(self, dataunit, dtype=None):
        pass


    '''
        Function to return label for the data source
    '''
    def get_label(self):
        return ''


