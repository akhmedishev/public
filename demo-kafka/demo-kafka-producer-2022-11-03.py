import sys
import time
import socket
import json
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import uuid

#Producer

prod_conf = {'bootstrap.servers': "ip:port",
        'client.id': socket.gethostname(),
        }

producer = Producer(prod_conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


with open(r'.\2022-11-03\ticket-to-send.xml', 'rb') as fin:
    message_body = fin.read()

#
#print(uuid.uuid4())
#print(message_body)
#print(message_body.decode('utf-8'))

destination_topic = 'ru.gov.some.destination'


#
#print(destination_topic in producer.list_topics().topics)
#print(json.dumps(producer.list_topics().topics, ensure_ascii=False, indent=4))
#exit()

message_key = str(uuid.uuid4())
print('message_key:', message_key)
producer.produce(destination_topic, key=message_key, value=message_body, callback=acked)
# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)

