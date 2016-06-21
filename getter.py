
"""
   get message from a qpid broker
"""
import sys

import qpid.messaging.exceptions

from Amqp.RoutingKey import RoutingKey
from Amqp.QpidWrapper import ConsumerListener, Producer
from protomsg import GpbMessage


class MessageGetter(object):
    """ class Getter """
    def __init__(self, output=sys.stdout, brief=False, producer=None):

        self.__output = output
        self.__brief = brief
        self.__producer = producer

        if __debug__:
            self.__force_output = True

        self.__exit = False


    def process(self, message):
        if message.subject:
            keystr = message.subject
        else:
            keystr = message.properties.get('x-amqp-0-10.routing-key')

        print 'Got:', keystr

        if self.__producer:
            self.__producer.send(keystr, message.content)

        if self.__brief:
            return

        if not message.content_type in ('application/x-protobuf',
                                    'application/octstream'):
            print message.content_type
            print 'Not a google protobuf message'
            return

        if self.__output != sys.stdout:
            self.__output.write('/@ routing key\n%s\n@/\n' % keystr)

        routingkey = RoutingKey(keystr)

        try:
            pmsg = GpbMessage(routingkey.package, routingkey.publication)
        except ImportError:
            self.__output.write('Unknown message {}'.format(routingkey))
            return


        from google.protobuf.message import DecodeError
        try:
            pmsg.loads(message.content)

            self.__output.write('{}'.format(pmsg))

            self.__output.write('@/\n\n')
            self.__output.flush()

        except DecodeError:
            print 'Decode error for key: %s' % keystr
            self.__output.write('Decode error for key: %s\n' % keystr)


