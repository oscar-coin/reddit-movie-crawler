import pymongo
import configparser
__author__ = 'Fabian'


class MongoConnection(object):

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('mongo_connection.ini')
        config = config['connection']
        self.c_uri = config['uri']
        self.c_db = config['db']
        self.client = pymongo.MongoClient(self.c_uri)
        self.db = self.client[self.c_db]

    def get_db(self):
        return self.db





