import socket
import sys
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

    OK_HTTP_RESPONSE = "HTTP/1.1 200 OK\n" + \
                       "Content-Type: application/json\n" + \
                       "\n" + \
                       "%s"

    BAD_HTTP_RESPONSE = "HTTP/1.1 400 Bad Request\n" + \
                        "Content-Type: application/json\n" + \
                        "\n" + \
                        "%s"

    def __init__(self, server_port=None, db_port=None, db_host=None):

        self.server_port = server_port or self.SERVER_PORT
        self.db_port = db_port or self.MONGO_PORT
        self.db_host = db_host or self.MONGO_HOST

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # create a file handler
        handler = logging.FileHandler('pyparty_server.log')
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter(
            '%(asctime)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(handler)

    def run_server(self):

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the port
        server_address = ('localhost', self.server_port)
        self.logger.info('starting up on %s port %s' % server_address)
        sock.bind(server_address)

        # Listen for incoming connections
        sock.listen(1)

        eventing_engine = Pyparty(self.db_host, self.db_port)

        while True:
            # Wait for a connection
            connection, client_address = sock.accept()

            try:
                self.logger.info('connection from %s' % client_address)

                new_data = connection.recv(1024)

                self.logger.info('received "%s"' % new_data)

                data = json.loads(new_data)

                if data["service"] == "subscribe":
                    sub = Subscription(data["subscriber_name"],
                                       data["subscriber_host"],
                                       data["subscriber_port"],
                                       data["subscriber_path"],
                                       data["publisher_name"],
                                       data["event_name"])

                    result = eventing_engine.subscribe(sub)

                    payload = json.dumps({"object_id": str(result)})

                elif data["service"] == "publish":
                    event = Event(data["publisher_name"],
                                  data["event_name"],
                                  data["event_description"])

                    threads = [threading.Thread(target=self.send_event,
                                                args=(subscriber.get_url(),
                                                      event.json))
                               for subscriber in eventing_engine.publish(event)]

                    # start threads
                    for t in threads:
                        t.start()

                    # wait for threads to finish
                    for t in threads:
                        t.join()

                    payload = json.dumps({"Status": 200})


                elif data["service"] == "unsubscribe":
                    sub = Subscription(data["subscriber_name"], data["subscriber_host"],
                                       data["subscriber_port"], data["subscriber_path"],
                                       data["publisher_name"], data["event_name"])

                    result = eventing_engine.unsubscribe(sub)

                    payload = json.dumps({"number_of_unsubscribes": result})

            except KeyError:
                payload = self.BAD_HTTP_RESPONSE % json.dumps(
                    {"Error": "Incorrect service"})
                self.logger.info("Status 400, KeyError - incorrect service")

            except ValueError:
                payload = self.BAD_HTTP_RESPONSE % \
                          json.dumps({"Error": "Incorrect JSON"})
                self.logger.info("Status 400, ValueError - incorrect JSON")

            finally:

                print >> sys.stderr, 'sending data back to the client'
                connection.sendall(payload)

                # Clean up the connection
                connection.close()

    def send_event(self, url, json_payload):
        r = requests.post(url, json=json_payload)

        if r.status_code == requests.codes.ok:
            self.logger.info("%s send success" % url)
        else:
            self.logger.info("%s send error" % url )


if __name__ == "__main__":

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

    server = PypartyServer(server_port=args.server_port,
                           db_port=args.mongo_port,
                           db_host=args.mongo_host)

    server.run_server()