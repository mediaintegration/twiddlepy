from twiddlepy import datasources
from twiddlepy.utils import logger

class DatasourceManager:
    def __init__(self, config):
        self.config = config
        self.datasource = None

    '''
        Function to return the data source object
        data source will be built if it has not been
    '''
    def get_datasource(self):
        if self.datasource is None:
            self.build_data_source()
        return self.datasource

    '''
        Function to build the data source object
    '''
    def build_data_source(self):
        # camelcase the datasource type
        ds_type = self.config['DataSource']['Type'].lower().title().replace('.', '')
        ds_cls_name = 'Ds' + ds_type
        logger.debug('Using %s', ds_cls_name)

        ds_cls = getattr(datasources, ds_cls_name, None)

        if ds_cls is not None:
            self.datasource = ds_cls(self.config)
        else:
            raise ValueError('Unrecognised datasource type "{}"'.format(self.config['DataSource']['Type']))
