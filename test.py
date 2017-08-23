from pyparty.pyparty import Pyparty, Subscription, Event
import threading

#pp = Pyparty('localhost', 27017)

#sub = Subscription("tom", "localhost", 22222, "", None, "ziooom")

# pp.subscribe(sub)

#print pp.unsubscribe(sub)
#
# eve = Event("alan", "ziooom", "fuck me, it's delicious!")
#
# for a in pp.publish(eve):
#     print a.get_url()



"""

url = "http://" + subscription["subscriber_host"] + ":" + \
      str(subscription["subscriber_port"]) + subscription["subscriber_path"]

r = requests.post(url, json=event.json)



if r.status_code == requests.codes.ok:
    pass
    # TODO it's ok!
else:
    pass
    # TODO it's not ok!
"""


def loop(n):
    for i in range(n):
        print i

# create threads
threads = [threading.Thread(target=loop, args=(15,))
           for i in range(10)]

# start threads
for t in threads:
    t.start()

# wait for threads to finish
for t in threads:
    t.join()

print len(threads)
