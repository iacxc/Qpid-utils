"""
   @@@ START COPYRIGHT @@@

   (C) Copyright 2013-2014 Hewlett-Packard Development Company, L.P.

   @@@ END COPYRIGHT @@@

   $Id: QpidWrapper.py 38908 2014-04-01 03:22:04Z cheng-xin.cai $

   provide the wrapper for qpid using qpid.messaging

"""

__all__ = []


import os
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



def main():
    """ main function for testing """
    import sys
    import Amqp
    from optparse import OptionParser

    sys.path.append(os.getenv('SEAPILOT_HOME') +
                       '/base/source/etc/publications')
    sys.path.append(os.getenv('SEAPILOT_HOME') + '/etc/publications')

    parser = OptionParser()
    parser.add_option('--consumer',  dest = 'consumer', action = 'store_true')
    parser.add_option('--producer',  dest = 'consumer', action = 'store_false')

    parser.add_option('--broker',  dest = 'broker', default = 'localhost:5672')

    (opts, _args) = parser.parse_args()

    from common.text_event_pb2 import text_event

    if opts.consumer:
        receiver = Consumer(opts.broker)

        import qpid.messaging.exceptions

        try:
            msg = receiver.fetch(60)
            msg_tev = text_event()
            msg_tev.ParseFromString(msg.content)
            print 'Got message', msg_tev
        except qpid.messaging.exceptions.Empty:
            print 'No message'
        except KeyboardInterrupt:
            print

        receiver.close()

    else:
        sender = Producer(opts.broker)

        msg = text_event()
        Amqp.initEventHeader(msg.header, 13, 100200, 3)
        msg.text = 'This is a test event'
        sender.send('common.text_event', msg.SerializeToString())

        sender.close()


if __name__ == '__main__':
    main()
