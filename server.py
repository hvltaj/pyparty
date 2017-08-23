import socket
import json
from pyparty.pyparty import Pyparty, Subscription, Event
import argparse
import requests
import threading
import logging


class PypartyServer(object):

    SERVER_PORT = 6666
    MONGO_PORT = 27017
    MONGO_HOST = 'localhost'

    OK_HTTP_RESPONSE = "200 OK\n" + \
                       "\n" + \
                       "%s"

    BAD_HTTP_RESPONSE = "400 Bad Request\n" + \
                        "\n" + \
                        "%s"

    def __init__(self, server_port=None, db_port=None, db_host=None):

        self.server_port = server_port or self.SERVER_PORT
        self.db_port = db_port or self.MONGO_PORT
        self.db_host = db_host or self.MONGO_HOST

    def run_server(self):

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the port
        server_address = ('localhost', self.server_port)
        logger.info('starting up on %s port %s' % server_address)
        sock.bind(server_address)

        # Listen for incoming connections
        sock.listen(1)

        eventing_engine = Pyparty(self.db_host, self.db_port)

        while True:
            # Wait for a connection
            connection, client_address = sock.accept()

            payload = ""

            try:
                logger.info('connection from %s:%s' % client_address)

                new_data = connection.recv(1024)

                logger.info('received "%s"' % new_data)

                data = json.loads(new_data)

                # Match service key to a given pyparty method.
                # Throws KeyError is service not present in the JSON request,
                # and ValueError is JSON is not properly constructed

                if data["service"] == "subscribe":
                    sub = Subscription(data["subscriber_name"],
                                       data["subscriber_host"],
                                       data["subscriber_port"],
                                       data["subscriber_path"],
                                       data["publisher_name"],
                                       data["event_name"])

                    result = eventing_engine.subscribe(sub)

                    payload = self.OK_HTTP_RESPONSE % \
                        json.dumps({"object_id": str(result)})

                    logger.info("%s Subscription successful" %
                                     json.dumps({"object_id": str(result)}))

                elif data["service"] == "publish":
                    event = Event(data["publisher_name"],
                                  data["event_name"],
                                  data["event_description"])

                    threads = [threading.Thread(target=self.send_event,
                                                args=(
                                                    self.get_url(subscriber),
                                                    json.dumps(
                                                        event._asdict()
                                                    )
                                                )
                                                )
                               for subscriber in eventing_engine.publish(event)
                               ]

                    # start threads
                    for t in threads:
                        t.start()

                    # wait for threads to finish
                    for t in threads:
                        t.join()

                    payload = self.OK_HTTP_RESPONSE % \
                        json.dumps({"publish_count": len(threads)})

                elif data["service"] == "unsubscribe":

                    result = eventing_engine.unsubscribe(
                        data["subscription_id"])

                    payload = self.OK_HTTP_RESPONSE % \
                        json.dumps({"deleted_count": result})

                    logger.info("%s unsubscribed %s" % (
                        data["subscription_id"], result))
                else:
                    raise KeyError("There is no matching service")

            except KeyError:
                payload = self.BAD_HTTP_RESPONSE % json.dumps(
                    {"Error": "Incorrect service"})
                logger.info("Status 400, KeyError - incorrect service")

            except ValueError:
                payload = self.BAD_HTTP_RESPONSE % \
                          json.dumps({"Error": "Incorrect JSON"})
                logger.info("Status 400, ValueError - incorrect JSON")

            finally:
                # print >> sys.stderr, 'sending data back to the client'
                connection.sendall(payload)

                # Clean up the connection
                connection.close()

    @staticmethod
    def send_event(url, json_payload):
        try:
            r = requests.post(url, json=json_payload)

            print r.status_code

            if r.ok:
                logger.info("%s send success" % url)
            else:
                logger.info("%s send error. response_code: %s" %
                                 (url, r.status_code))
        except requests.ConnectionError:
            logger.info("%s Connection error" % url)

    @staticmethod
    def get_url(subscription, protocol=None):
            """ Returns a subscriber url"""

            if not protocol:
                protocol = "http://"

            return protocol + subscription.subscriber_host + ":" + \
                str(subscription.subscriber_port) + \
                subscription.subscriber_path

if __name__ == "__main__":
    """ Non default ports and hosts may be set up using command line """

    parser = argparse.ArgumentParser(description='Run pyparty Event Engine '
                                                 'server.')
    parser.add_argument('--server_port', type=int,
                        help='Port you want the server to run on. Default is '
                             '6666')
    parser.add_argument('--mongo_port', type=int,
                        help='Port on which the MongoDB is running. Default is'
                             ' 27017')
    parser.add_argument('--mongo_host',
                        help='Host (ip or URL) on which the MongoDB is '
                             'running. Default is <localhost>')

    args = parser.parse_args()

    """ Initialize and set up logging """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('pyparty_server.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    server = PypartyServer(server_port=args.server_port,
                           db_port=args.mongo_port,
                           db_host=args.mongo_host)

    server.run_server()
