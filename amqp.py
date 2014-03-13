
# $Id: amqp.py 325 2013-08-15 05:44:10Z chengxin.cai $

__all__ = ('RoutingKey', 'Producer', 'Consumer')

import os,sys

from qpid.messaging import Connection, Message


DEF_CONTENTTYPE = 'application/x-protobuf'

class RoutingKey(object):

    def __init__(self, key = None):
        if key:
           self._set(key)

    def _set(self, key):
        items = key.split('.')
        if len(items) == 6:
        # category, package, scope, security, protocol and publication
            self.__category = items[0]
            self.__package  = items[1]
            self.__scope    = items[2]
            self.__security = items[3]
            self.__protocol = items[4]
            self.__publication = items[5]

        elif len(items) == 5:
        # category, package, scope, security and publication
            self.__category = items[0]
            self.__package  = items[1]
            self.__scope    = items[2]
            self.__security = items[3]
            self.__protocol = 'gpb'
            self.__publication = items[4]

        elif len(items) == 4:
        # package, scope, security and publication
            self.__category = 'health_state'
            self.__package  = items[0]
            self.__scope    = items[1]
            self.__security = items[2]
            self.__protocol = 'gpb'
            self.__publication = items[3]

        elif len(items) == 3:
        # category, package and publication
            self.__category = items[0]
            self.__package  = items[1]
            self.__scope    = 'instance'
            self.__security = 'public'
            self.__protocol = 'gpb'
            self.__publication = items[2]

        elif len(items) == 2:
        # package and publication
            self.__category = 'health_state'
            self.__package  = items[0]
            self.__scope    = 'instance'
            self.__security = 'public'
            self.__protocol = 'gpb'
            self.__publication = items[1]

        else:
            raise TypeError('field number mismatch in %s' % key)

        if self.__protocol != 'gpb':
            raise TypeError('Protocol error %s' % key)

    #properties
    def GetCategory(self):
        return self.__category

    def GetPackage(self):
        return self.__package

    def GetScope(self):
        return self.__scope

    def GetSecurity(self):
        return self.__security

    def GetProtocol(self):
        return self.__protocol

    def GetPublicationName(self):
        return self.__publication

    #attributes
    def GetAsSubscribeString(self):
        return '#.%s.#.%s' % (self.__package, self.__publication)

    def GetAsMessageName(self):
        return '%s.%s' % (self.__package, self.__publication)

    def __repr__(self):
        return '.'.join([self.__category, self.__package, self.__scope,
                       self.__security, self.__protocol, self.__publication])



class QpidWrapper(object):
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
        return self.__session


    def create_address(self, routing_key=None):
        if routing_key:
            return '%s/%s;%s' % (self.__exchange, routing_key,
                                 self.__address_opt)
        else:
            return '%s;%s' % (self.__exchange, self.__address_opt)


    def close(self):
        self.__session.close()
        self.__conn.close()


class Producer(QpidWrapper):

    def __init__(self, broker='localhost:5672', exchange='amq.topic',
                       username=None, password=None):
        super(Producer, self).__init__(broker, exchange, username, password)

        self.__sender = self.session.sender(self.create_address())


    def send(self, routing_key, msg_str):
        msg = Message(subject=routing_key,
                      content_type = DEF_CONTENTTYPE,
                      content=msg_str)

        if __debug__: print 'Sending', msg

        self.__sender.send(msg)


class Consumer(QpidWrapper):

    def __init__(self, broker="localhost:5672", exchange="amq.topic",
                       binding_keys=[], username=None, password=None):
        super(Consumer, self).__init__(broker, exchange, username, password)

        for k in binding_keys:
            if __debug__: print 'Binding %s on %s' %(k, broker)

            self.session.receiver(self.create_address(k), capacity=200)


    def fetch(self, timeout=None):
        message = self.session.next_receiver(timeout).fetch(timeout)

        if __debug__: print 'Got message', message

        self.session.acknowledge(message)

        return message


#----- main ------
def main():
    import sys

    host = 'localhost' if len(sys.argv) < 2 else sys.argv[1]
    port = 5672 if len(sys.argv) < 3 else int(sys.argv[2])

    sender = Producer('%s:%d' %(host, port))

    key = RoutingKey('vertica.event_query')
    msgstr = 'Hello World'
    sender.send(str(key), msgstr)

    receiver = Consumer('%s:%d' %(host, port),
                        binding_keys=[key.GetAsSubscribeString()])

    import qpid.messaging.exceptions

    try:
        msg = receiver.fetch(10)
        print 'Got message', msg
    except qpid.messaging.exceptions.Empty:
        print 'No message'
        sys.exit(0)
    except KeyboardInterrupt:
        print
        sys.exit(0)


if __name__ == '__main__':
    main()
