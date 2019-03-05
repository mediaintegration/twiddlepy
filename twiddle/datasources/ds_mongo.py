from .ds_base import DsBase
from pymongo import MongoClient
import pandas as pd

from twiddle.utils import logger

class DsMongo(DsBase):

    def __init__(self, ds_type, config):
        super().__init__(ds_type, config)

        ds_config = config[self.ds_config_section]

        self.mongo_server = ds_config["MongoServer"]
        # if not self.mongo_server:
        #     self.mongo_server = "localhost"

        self.mongo_port = ds_config["MongoPort"]
        # if not self.mongo_port:
        #     self.mongo_port = 27017

        if not self.mongo_username or not self.mongo_password:
            logger.warn("Username and/or password not set, not using authentication")

        self.mongo_database = ds_config["MongoDatabase"]
        self.mongo_collection = ds_config["MongoCollection"]
        if not self.mongo_database or not self.mongo_collection:
            logger.error("Mongo database and/or collection not defined in configuration")
            raise Exception("Mongo database and/or collection not defined in configuration")

        self.mongo_query = ds_config["MongoQuery"]

        # We don't want to default loading all data from a collection,
        # so let's require a query for now (user can still specify a query all
        # if they want to)
        if not self.mongo_query:
            logger.error("Failed to load MongoQuery from configuration")
            raise Exception("Failed to load MongoQuery from configuration")

        self.mongo_username = ds_config["MongoUsername"]
        self.mongo_password = ds_config["MongoPassword"]
        self.mongo_client = MongoClient(self.mongo_server, self.mongo_port)

    def read_data_to_df(self, tablename, dtype=None):
        database = self.mongo_client[self.mongo_database]
        collection = database[self.mongo_collection]

        documents = collection.find(self.mongo_query)
        
        # TODO: Remove this statement or refine into more of a 
        # debug statement than just printing all the documents to screen.
        logger.debug(documents)

        df = pd.from_dict(documents)

        return df


