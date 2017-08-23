import socket
import sys
import json
from pyparty.pyparty import Pyparty, Subscription, Event
import argparse


class PypartyServer(object):

    SERVER_PORT = 6666
    MONGO_PORT = 27017
    MONGO_HOST = 'localhost'

    def __init__(self, server_port=None, db_port=None, db_host=None):

        self.server_port = server_port or self.SERVER_PORT
        self.db_port = db_port or self.MONGO_PORT
        self.db_host = db_host or self.MONGO_HOST

    def run_server(self):

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the port
        server_address = ('localhost', self.server_port)
        print >>sys.stderr, 'starting up on %s port %s' % server_address
        sock.bind(server_address)

        # Listen for incoming connections
        sock.listen(1)

        eventing_engine = Pyparty(self.db_host, self.db_port)

        while True:
            # Wait for a connection
            print >>sys.stderr, 'waiting for a connection'
            connection, client_address = sock.accept()

            try:
                print >> sys.stderr, 'connection from', client_address

                # Receive the data in small chunks and retransmit it
                while True:
                    payload = ""
                    new_data = connection.recv(1024)

                    print >> sys.stderr, 'received "%s"' % new_data

                    data = json.loads(new_data)

                    try:
                        if data["service"] == "subscribe":
                            sub = Subscription(data["subscriber_name"], data["subscriber_host"],
                                               data["subscriber_port"], data["subscriber_path"],
                                               data["publisher_name"], data["event_name"])

                            result = eventing_engine.subscribe(sub)

                            payload = json.dumps({"object_id": str(result)})

                        elif data["service"] == "publish":
                            pass
                        elif data["service"] == "unsubscribe":
                            sub = Subscription(data["subscriber_name"], data["subscriber_host"],
                                               data["subscriber_port"], data["subscriber_path"],
                                               data["publisher_name"], data["event_name"])

                            result = eventing_engine.unsubscribe(sub)

                            payload = json.dumps({"number_of_unsubscribes": result})

                    except KeyError:
                        pass

                    print >> sys.stderr, 'sending data back to the client'
                    connection.sendall(payload)

                    break

            finally:
                # Clean up the connection
                connection.close()

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

    server = PypartyServer(server_port=args.get("server_port"),
                           db_port=args.get("mongo_port"),
                           db_host=args.get("mongo_host"))

    server.run_server()