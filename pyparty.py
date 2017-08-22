import requests
from pymongo import MongoClient


class Pyparty(object):

    def __init__(self, mongo_host, mongo_port):

        self.mongo_host = mongo_host
        self.mongo_port = mongo_port

        # setup MongoDB
        self.client = MongoClient(self.mongo_host, self.mongo_port)

        # load DB and table. Both will be initialized if don't exist
        self.db = self.client['EventsDB']
        self.collection = self.db['Events']

    def subscribe(self, subscriber_name, ):
        pass

    def publish(self, event):
        pass


class Event(object):

    def __init__(self, publisher_name, event_name, event_description):

        self.publisher_name = publisher_name
        self.event_name = event_name
        self.event_description = event_description


class Subscription(object):

    def __init__(self):
        pass

