from .ds_base import DsBase
from pymongo import MongoClient
import pandas as pd
from ast import literal_eval

from twiddlepy.utils import logger

class DsMongo(DsBase):

    def __init__(self, config):
        super().__init__('mongo', config)

        ds_config = config[self.ds_config_section]

        self.mongo_server = ds_config["MongoServer"]
        # The port must be an integer, so convert it to int
        self.mongo_port = int(ds_config["MongoPort"])
    
        self.mongo_database = ds_config["MongoDatabase"]
        self.mongo_collection = ds_config["MongoCollection"]
        if not self.mongo_database or not self.mongo_collection:
            logger.error("Mongo database and/or collection not defined in configuration")
            raise Exception("Mongo database and/or collection not defined in configuration")

        self.mongo_query = literal_eval(ds_config["MongoQuery"])
        logger.debug("Mongo Query: %s", self.mongo_query)

        # # We don't want to default loading all data from a collection,
        # # so let's require a query for now (user can still specify a query all
        # # if they want to)
        # if not self.mongo_query:
        #     logger.error("Failed to load MongoQuery from configuration")
        #     raise Exception("Failed to load MongoQuery from configuration")

        self.mongo_username = ds_config["MongoUsername"]
        self.mongo_password = ds_config["MongoPassword"]
        if not self.mongo_username or not self.mongo_password:
            logger.warn("Username and/or password not set, not using authentication")

        self.mongo_client = MongoClient(self.mongo_server, self.mongo_port)

    def read_data_to_df(self, tablename, dtype=None):
        database = self.mongo_client[self.mongo_database]
        collection = database[self.mongo_collection]

        documents = collection.find(self.mongo_query)

        df = pd.DataFrame.from_dict(documents)
        logger.debug(df)

        return df

    def get_data_units(self):
        return [self.mongo_collection]


