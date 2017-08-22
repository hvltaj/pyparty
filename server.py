import socket
import sys
import json
from pyparty.pyparty import Pyparty, Subscription, Event


def run_server():

    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the port
    server_address = ('localhost', 6666)
    print >>sys.stderr, 'starting up on %s port %s' % server_address
    sock.bind(server_address)

    # Listen for incoming connections
    sock.listen(1)

    eventing_engine = Pyparty('localhost', 27017)

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
    run_server()