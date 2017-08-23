PyParty - Python Eventing Engine

PyParty offers you to subscribe for events basing on 2 arguments: event_name or
publisher_name. All subscriptions are stored in a Mongo database. Once publish
request is made, the engine looks for all subscriptions that match the
publishing event. At the end server sends the event to all subscribed addresses.
There is also possibility to list all your subscriptions and unsubscribe.

sample requests and responses:

> subscribe

request:
    
    {
    "subscriber_name": "filip",
    "subscriber_host": "localhost",
    "subscriber_port": 22222,
    "subscriber_path": "",
    "publisher_name": "alan",
    "event_name": "ziooom",
    "service": "subscribe"
    }

response:
    
    200 OK

    {"object_id": "599da72edebc474c3ec2571b"}


> publish

request:
    
    {
    "event_description": "Olaboga, co to za event!",
    "publisher_name": "turbot",
    "event_name": "ziooom",
    "service": "publish"
    }

response:
    
    200 OK

    {"publish_count": 17}


> unsubscribe

request:
    
    {
    "service": "unsubscribe",
    "subscription_id": "599d6e1adebc471061dbcf3c"
    }

response:
    
    200 OK

    {"deleted_count": 0}
