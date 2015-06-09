#!/usr/bin/env python
import pika
import cPickle as pickle

loads = pickle.loads
dumps = pickle.dumps


class Worker():
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
        self.channel = self.connection.channel()

    def brpop(self, queue, process_message_func):
        self.channel.queue_declare(queue, durable=True, passive=True)

        def callback(ch, method, properties, body):
            print " [x] Received %r" % (body,)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if body:
                process_message_func(loads(body)[0])
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(callback, queue=queue)

        self.channel.start_consuming()
