from pymongo import MongoClient
import datetime
from bson.objectid import ObjectId
from collections import namedtuple


class MongoDBConnection(object):
    """MongoDB Connection Context Manager"""

    def __init__(self, host='localhost', port='27017'):
        self.host = host
        self.port = port
        self.connection = None

    def __enter__(self):
        self.connection = MongoClient(self.host, self.port)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


class Pyparty(object):
    """ Events engine based on MongoDB. Allows to subscribe for a given
    type of an event or a event author, list your subscriptions, unsubscribe
    based on subscription ID and publish events """

    def __init__(self, mongo_host, mongo_port):
        """ Initialize connection to the MongoDB """

        self.mongo = MongoDBConnection(mongo_host, mongo_port)

    def subscribe(self, subscription):
        """ Subscribe for an event """

        post = {
                "subscriber_name": subscription.subscriber_name,
                "subscriber_host": subscription.subscriber_host,
                "subscriber_port": subscription.subscriber_port,
                "subscriber_path": subscription.subscriber_path,
                "publisher_name": subscription.publisher_name,
                "event_name": subscription.event_name,
                "date": datetime.datetime.now()
                }

        with self.mongo:

            collection = self.mongo.connection['EventsDB']['Subscriptions']

            post_id = collection.insert_one(post).inserted_id

            return post_id

    def unsubscribe(self, subscription_id):
        """ Unsubscribe based on the given subscription_id """

        query = {
                "_id": {"$eq": ObjectId(subscription_id)}
            }

        with self.mongo:
            collection = self.mongo.connection['EventsDB']['Subscriptions']

            result = collection.delete_one(query)
            return result.deleted_count

    def publish(self, event):
        """ Publish an event. Generates a list of subscriptions that have
        publisher_name or event_name matching the publishing event """

        query = {"$or": [
                {
                    "publisher_name": {"$eq": event.publisher_name}
                },
                {
                    "event_name": {"$eq": event.event_name}
                }
        ]}

        with self.mongo:
            collection = self.mongo.connection['EventsDB']['Subscriptions']

            for subscription in collection.find(query):

                sub = Subscription(subscription["subscriber_name"],
                                   subscription["subscriber_host"],
                                   subscription["subscriber_port"],
                                   subscription["subscriber_path"],
                                   subscription["publisher_name"],
                                   subscription["event_name"])

                yield sub

    def list_subscriptions(self, subscriber_name):
        """ Lists subscriptions based on subscriber_name value """

        query = {
                "subscriber_name": {"$eq": subscriber_name}
        }

        with self.mongo:
            collection = self.mongo.connection['EventsDB']['Subscriptions']

            for subscription in collection.find(query):

                sub = Subscription(subscription["subscriber_name"],
                                   subscription["subscriber_host"],
                                   subscription["subscriber_port"],
                                   subscription["subscriber_path"],
                                   subscription["publisher_name"],
                                   subscription["event_name"])

                yield sub


""" Store all info about an Event"""

Event = namedtuple("Event", 'publisher_name event_name event_description')

""" Store all info about a Subscription"""

Subscription = namedtuple("Subscription", 'subscriber_name subscriber_host '
                                          'subscriber_port subscriber_path '
                                          'publisher_name event_name')


