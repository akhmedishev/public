import sys
import time
import socket
import json
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

prod_conf = {'bootstrap.servers': "id:port",
        'client.id': socket.gethostname()}

def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))

cons_conf = {'bootstrap.servers': "id:port",
        'group.id': "demo-kafka-pg",
        'enable.auto.commit': False,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'on_commit': commit_completed
    }

running = True

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        #MIN_COMMIT_COUNT = 1
        msg_count = 0
        while running:
            msg = consumer.poll(timeout=2.0)
            if msg is None: continue
            dt_now = datetime.now()
            saveerr = msg.error()
            if isinstance(saveerr, KafkaError):
                saveerr = dict(
                            code        = msg.error().code     (),
                            fatal       = msg.error().fatal    (),
                            name        = msg.error().name     (),
                            retriable   = msg.error().retriable(),
                            str         = msg.error().str      (),
                            txn_requires_abort = msg.error().txn_requires_abort()
                        )
            #print(type(msg.value    ()))
            #print((msg.value    ().decode('utf-8')))
            mv = msg.value()
            mk = msg.key()
            if isinstance(mv, bytes):
                with open(dt_now.strftime('message-%Y-%d-%m-%H%M%S.%f-value.bin'),
                          'wb') as fout:
                    fout.write(mv)
                mv = mv.decode('utf-8')
            if isinstance(mk, bytes):
                with open(dt_now.strftime('message-%Y-%d-%m-%H%M%S.%f-key.bin'),
                          'wb') as fout:
                    fout.write(mk)
                mk = mk.decode('utf-8')
            with open(dt_now.strftime('message-%Y-%d-%m-%H%M%S.%f.json'),
                      'w', encoding='utf-8') as fout:
                json.dump(dict(
                    error     = saveerr,
                    headers   = msg.headers  (),
                    key       = mk,
                    latency   = msg.latency  (),
                    offset    = msg.offset   (),
                    partition = msg.partition(),
                    timestamp = msg.timestamp(),
                    topic     = msg.topic    (),
                    value     = mv,
                    ), fout, ensure_ascii=False, indent=4)
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART):
                    # End of partition event
                    sys.stderr.write('%% %s [%d] %s at offset %s\n' %
                                     (msg.topic(), msg.partition(), msg.error(), msg.offset()))
                    time.sleep(120)
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                #msg_process(msg)
                msg_count += 1
                #if msg_count % MIN_COMMIT_COUNT == 0:
                if msg_count:
                    consumer.commit(asynchronous=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

consumer = Consumer(cons_conf)

#
# print(consumer.list_topics().brokers)
# print(consumer.list_topics().topics)


source_topic = "ru.gov.some.source"


while source_topic not in consumer.list_topics().topics:
    print('Topic', source_topic, 'does not exists.')
    time.sleep(600)

consume_loop(consumer, [source_topic])

time.sleep(20)

shutdown()