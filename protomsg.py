
# $Id: protomsg.py 290 2013-07-28 11:48:00Z chengxin.cai $

""" Provide all needed functions, classes for handle google protocol buffer
"""

import os, sys
import time
import socket
import thread

__all__ = ()

import Amqp

def _find_field(root, attrs):
    """ find the final GPB field msg"""
    if len(attrs) == 0:
        return root

    return _find_field(getattr(root, attrs[0]), attrs[1:])

def _get_topo(msg):
    """ get the topology for a GPB message """
    topo = {}
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:

        fd_type   = fdesc.cpp_type
        fd_name   = fdesc.name
        full_name = fdesc.full_name

        field = getattr(msg, fd_name)

        if fd_type == fdesc.CPPTYPE_MESSAGE:
            if getattr(field, '_values', None) is None: # not repeated
                the_topo = _get_topo(getattr(msg, fd_name))
            else:
                the_topo = _get_topo(getattr(msg, fdesc.message_type.name))
        else:
            typename = {
                fdesc.CPPTYPE_INT32  : 'int32',
                fdesc.CPPTYPE_UINT32 : 'uint32',
                fdesc.CPPTYPE_INT64  : 'int64',
                fdesc.CPPTYPE_UINT64 : 'uint64',
                fdesc.CPPTYPE_DOUBLE : 'double',
                fdesc.CPPTYPE_FLOAT  : 'float',
                fdesc.CPPTYPE_STRING : 'string'
            }
            the_topo = typename.get(fd_type, 'unknown')

        if getattr(field, '_values', None) is None: # not repeated
            topo[full_name] = the_topo
        else:
            topo[full_name] = [the_topo]

    return topo

def _fill_from_json(msg, adict):
    """ fill the msa from a dictionary """
    for fd_name, fd_value in adict.iteritems():
        if isinstance(fd_value, dict):
            _fill_from_json(getattr(msg, fd_name), fd_value)
        elif isinstance(fd_value, list):
            field = getattr(msg, fd_name)
            for v in fd_value:
                if isinstance(v, dict):
                    _fill_from_json(field.add(), v)
                else: # simple value
                    field.append(v)
        else: # simple value
            setattr(msg, fd_name, fd_value)

def _fill_msg(msg, tokens, component_id):
    """ fill the msg from a token list """
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:
        token = tokens.popleft() if len(tokens) > 0 else None

        if token:
            fd_name, fd_type = fdesc.name, fdesc.cpp_type
            converter = {
                fdesc.CPPTYPE_ENUM : int,
                fdesc.CPPTYPE_INT32 : int,
                fdesc.CPPTYPE_UINT32 : int,
                fdesc.CPPTYPE_INT64 : int,
                fdesc.CPPTYPE_UINT64 : int,
                fdesc.CPPTYPE_DOUBLE : float,
                fdesc.CPPTYPE_FLOAT : float,
                fdesc.CPPTYPE_MESSAGE : None,
                fdesc.CPPTYPE_STRING : lambda x: x
                # otherwise: CPPTYPE_BOOL
            }

            field = getattr(msg, fd_name)
            if getattr(field, '_values', None) is None: # not repeated
                if fd_type == fdesc.CPPTYPE_MESSAGE:
                    if fdesc.message_type.name == 'info_header':
                        Amqp.init_info_header(msg.header, component_id)
                    else:
                        tokens.appendleft(token)  # push back the token
                        _fill_msg(field, tokens, component_id)
                else: # simple types
                    try:
                        setattr(msg, fd_name, converter.get(fd_type, str)(token))
                    except ValueError, e:
                        print e
                        print 'Error in', fd_name

            else: # repeated field
                if fd_type == fdesc.CPPTYPE_MESSAGE:
                    for _ in range(int(token)):
                        _fill_msg(field.add(), tokens, component_id)

                else: # simple types
                    field.append(converter.get(fd_type, str)(token))

def _extract_msg(msg):
    """ extract the msg to a token list """
    tokens = []
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:
        fd_name, fd_type = fdesc.name, fdesc.cpp_type

        field = getattr(msg, fd_name)
        if getattr(field, '_values', None) is None: # not repeated
            if fd_type == fdesc.CPPTYPE_MESSAGE:
                if fdesc.message_type.name == 'info_header':
                    tokens.append('<header>')
                else:
                    tokens.extend(_extract_msg(field))
            else: # simple types
                tokens.append(str(getattr(msg, fd_name)))

        else: # repeated field
            tokens.append(str(len(field)))
            for subfield in field:
                if fd_type == fdesc.CPPTYPE_MESSAGE:
                    tokens.extend(_extract_msg(subfield))
                else:
                    tokens.append(str(subfield))

    return tokens

def _get_msg(msg):
    """ get the value for a message as a dict """
    msg_dict = {}
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:
        fd_name, fd_type = fdesc.name, fdesc.cpp_type
        field = getattr(msg, fd_name)

        if getattr(field, '_values', None) is None: # not repeated
            if fd_type == fdesc.CPPTYPE_MESSAGE:
                msg_dict[fd_name] = _get_msg(field)
            else:
                msg_dict[fd_name] = field
        else:# repeated
            if fd_type == fdesc.CPPTYPE_MESSAGE:
                msg_dict[fd_name] = [ _get_msg(fd) for fd in field ]
            else:
                msg_dict[fd_name] = getattr(field, '_values')

    return msg_dict

class GpbMessage(object):
    """ A wrapper class for GPB Message"""

    #instance properties and methods
    __slots__ = ( '__proto_path', '__pkg_name', '__msg_name', '__msg' )

    def __init__( self, proto_path, pkg_name, msg_name ):
        """ constructor """
        self.__proto_path = proto_path
        self.__pkg_name = pkg_name
        self.__msg_name = msg_name
        self.__msg      = None
        self._import()

    def __repr__(self):
        """ to representation """
        return self.__msg.__str__()

    def _import(self):
        """ import the message class, and create a instance in self.__msg """
        sys.path.append(self.__proto_path)
        module = __import__('%s.%s_pb2' % (self.__pkg_name, self.__msg_name),
                            globals(), locals(), [self.__msg_name])

        del sys.path[0]

        klass = getattr(module, self.__msg_name)

        self.__msg = klass()

    def get_field(self, field_name):
        """ get the field msg, according to the field_name """

        field_names = field_name.split('.')
        return _find_field(self.__msg, field_names)

    def field_value(self, field_name):
        """ get a value from the GPB message """

        return self.get_field(field_name)

    def set_field_value(self, field_name, value):
        """ set the value to GPB message """
        field_names = field_name.split('.')

        # find the parent field
        if len(field_names) == 1:
            field = self.__msg
        else:
            field = self.get_field('.'.join(field_names[:-1]))

        setattr(field, field_names[-1], value)

    def add_field_values(self, field_name, values):
        """ add the value list to GPB message, only work for simple variables
        """
        # we need the actual field
        field = self.get_field(field_name)

        field.extend(values)

    def loadtabs(self, astring, component_id):
        """ feed by a TAB splitted string """

        if __debug__:
            print 'component_id', component_id

        import Queue
        tokens = Queue.deque(astring.replace('\\t', '\t').split('\t'))

        _fill_msg(self.__msg, tokens, component_id)

    def loadjson(self, adict):
        _fill_from_json(self.__msg, adict)

    def loads(self, astring):
        """ wrapper for GPB's ParseFromString """
        self.__msg.ParseFromString(astring)

    def dumps(self):
        """ wrapper for GPB's SerializedToString """
        return self.__msg.SerializeToString()

    def topo(self):
        """ get the topology for the GPB message, return a dict """
        return _get_topo(self.__msg)

    def dumpjson(self):
        """ return a dict containing all values """
        return _get_msg(self.__msg)

    def dumptabs(self):
        """ return a string send to tpa_publish """
        tokens = _extract_msg(self.__msg)
        return '\t'.join(tokens)

    @property
    def message_name(self):
        """ return the message name """
        return '%s.%s' % (self.__pkg_name, self.__msg_name)

    @property
    def rawmsg(self):
        """ return the self.__msg """
        return self.__msg

if __name__ == '__main__':
    print 'Gpbmessage'

