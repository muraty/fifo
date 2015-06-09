import pika
import cPickle as pickle

loads = pickle.loads
dumps = pickle.dumps


class Sender():
    def __init__(self, broker):

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
        self.channel = self.connection.channel()

    def push_to_queue(self, queue, tasks):
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.exchange_declare(exchange='broker', type='fanout')
        self.channel.queue_bind(exchange='broker', queue=queue)
        for task in tasks:
            self.channel.basic_publish(exchange='broker',
                                       routing_key='routing_key',
                                       body=dumps(tasks),
                                       properties=pika.BasicProperties(
                                                      delivery_mode=2)
                                       )
        self.connection.close()
