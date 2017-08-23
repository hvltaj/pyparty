from pymongo import MongoClient
import datetime
from bson.objectid import ObjectId


class Pyparty(object):
    """ Events engine based on MongoDB. Allows to subscribe for a given
    type of an event or a event author, list your subscriptions, unsubscribe
    based on subscription ID and publish events """

    def __init__(self, mongo_host, mongo_port):
        """ Initialize connection to the MongoDB """

        self.mongo_host = mongo_host
        self.mongo_port = mongo_port

        # setup MongoDB
        self.client = MongoClient(self.mongo_host, self.mongo_port)

        # load DB and table. Both will be initialized if don't exist
        self.db = self.client['EventsDB']
        self.db_subscriptions = self.db['Subscriptions']

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

        post_id = self.db_subscriptions.insert_one(post).inserted_id
        return post_id

    def unsubscribe(self, subscription_id):
        """ Unsubscribe based on the given subscription_id """

        query = {
                "_id": {"$eq": ObjectId(subscription_id)}
            }

        result = self.db_subscriptions.delete_one(query)
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

        for subscription in self.db_subscriptions.find(query):

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

        for subscription in self.db_subscriptions.find(query):

            sub = Subscription(subscription["subscriber_name"],
                               subscription["subscriber_host"],
                               subscription["subscriber_port"],
                               subscription["subscriber_path"],
                               subscription["publisher_name"],
                               subscription["event_name"])

            yield sub

    def refresh_db(self):

        self.db = self.client['EventsDB']
        self.db_subscriptions = self.db['Subscriptions']


class Event(object):
    """ Data class. Holds information about an event"""

    def __init__(self, publisher_name, event_name, event_description):

        self.publisher_name = publisher_name
        self.event_name = event_name
        self.event_description = event_description

        self.json = {
            "publisher_name": self.publisher_name,
            "event_name": self.event_name,
            "event_description": self.event_description
        }


class Subscription(object):
    """ Data class. Holds information about a subscription"""

    def __init__(self, subscriber_name, subscriber_host, subscriber_port,
                 subscriber_path, publisher_name=None, event_name=None):

        self.subscriber_name = subscriber_name
        self.subscriber_host = subscriber_host
        self.subscriber_path = subscriber_path
        self.subscriber_port = subscriber_port
        self.publisher_name = publisher_name
        self.event_name = event_name

        if not (publisher_name or event_name):
            raise ValueError("publisher_name or event_name value has to be "
                             "provided in order to make a subscription ")

    def get_url(self, protocol=None):
        """ Returns a subscriber url"""

        if not protocol:
            protocol = "http://"

        return protocol + self.subscriber_host + ":" + \
            str(self.subscriber_port) + self.subscriber_path


