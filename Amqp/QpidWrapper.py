"""
   $Id: QpidWrapper.py 38908 2014-04-01 03:22:04Z cheng-xin.cai $

   provide the wrapper for qpid using qpid.messaging

"""

__all__ = []


import os
from threading import Thread
from qpid.messaging import Connection, Message

DEF_CONTENTTYPE = 'application/x-protobuf'

class QpidWrapper(object):
    """Base class
    """
    __address_opt = '{create:always, node:{type:topic, durable:True}}'

    def __init__(self, broker, exchange, username, password):

        self.__conn = Connection(broker,
                                 username=username,
                                 password=password,
                                 tcp_nodelay=True,
                                 transport='tcp')

        self.__conn.open()
        self.__session = self.__conn.session()

        self.__exchange = exchange

    @property
    def session(self):
        """ return the session """
        return self.__session


    def create_address(self, routing_key=None):
        """ create an address for sending/receiving message """
        if routing_key:
            return '{0}/{1};{2}'.format(self.__exchange, routing_key,
                                        self.__address_opt)
        else:
            return '{0};{1}'.format(self.__exchange,
                                    self.__address_opt)


    def close(self):
        """ close the connection to the qpid broker """
        self.__session.close()
        self.__conn.close()


class Consumer(QpidWrapper):
    """ the consumer """
    def __init__(self, broker="localhost:5672", exchange="amq.topic",
                       binding_keys=None, username=None, password=None):
        super(Consumer, self).__init__(broker, exchange, username, password)

        if binding_keys is None:
            binding_keys = ['#']

        for key in binding_keys:
            if __debug__:
                print 'Binding', key, 'on', broker

            self.session.receiver(self.create_address(key), capacity=200)


    def fetch(self, timeout=None):
        """ get a message """
        message = self.session.next_receiver(timeout).fetch(timeout)

        self.session.acknowledge(message)

        return message


class Producer(QpidWrapper):
    """ the consumer """
    def __init__(self, broker='localhost:5672', exchange='amq.topic',
                       username=None, password=None):
        super(Producer, self).__init__(broker, exchange, username, password)

        self.__sender = self.session.sender(self.create_address())


    def send(self, routing_key, msg_str, msg_type=DEF_CONTENTTYPE):
        """ send a message """
        msg = Message(subject=routing_key,
                      content_type = msg_type,
                      content=msg_str)

        if __debug__:
            print 'Sending', msg

        self.__sender.send(msg)


class ConsumerListener(Thread):
    def __init__(self, broker="localhost:5672", exchange="amq.topic",
                       binding_keys=None, username=None, password=None):
        super(ConsumerListener, self).__init__()
        self.__consumer = Consumer(broker, exchange, binding_keys,
                                           username, password)
        self.__exit = False

    def run(self):
        import qpid.messaging.exceptions

        while not self.__exit:
            try:
                message = self.__consumer.fetch(5)
                self.received(message)
            except qpid.messaging.exceptions.Empty:
                if __debug__:
                    print 'No message, continue'

        if __debug__:
            print 'Finished'


    def received(self, message):
        raise NotImplementedError, 'not implemented'


    def stop(self):
        self.__exit = True


def main():
    """ main function for testing """
    import sys
    import time
    import Amqp
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('--broker',  dest = 'broker', default = 'localhost:5672')
    parser.add_option('--consumer',  dest = 'consumer', action = 'store_true')
    parser.add_option('--producer',  dest = 'consumer', action = 'store_false')

    (opts, _args) = parser.parse_args()

    sys.path.append(os.getenv('HOME') + '/publications')

    from common.text_event_pb2 import text_event

    if opts.consumer:
        class TestListener(ConsumerListener):
            def __init__(self, broker="localhost:5672", exchange="amq.topic",
                       binding_keys=None, username=None, password=None):
                super(TestListener, self).__init__(broker, exchange,
                                        binding_keys, username, password)

            def received(self, message):
                msg_tev = text_event()
                msg_tev.ParseFromString(message.content)
                print 'Got message', msg_tev

        tl = TestListener(opts.broker)
        tl.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            print '\nControl-C pressed, waiting consumer to exit'
            tl.stop()

        tl.join()

    else:
        producer = Producer(opts.broker)

        msg_tev = text_event()
        Amqp.init_event_header(msg_tev.header, 13, 100200, 3)
        msg_tev.text = 'This is a test event'
        producer.send('common.text_event', msg_tev.SerializeToString())

        producer.close()


if __name__ == '__main__':
    main()
