
# $Id: protomsg.py 290 2013-07-28 11:48:00Z chengxin.cai $

""" Provide all needed functions, classes for handle google protocol buffer
"""

import re

__all__ = ('GpbMessage',
           )

import Amqp
import json


def _find_field(root, attrs):
    """ find the final GPB field msg"""
    if len(attrs) == 0:
        return root

    return _find_field(getattr(root, attrs[0]), attrs[1:])

    
def _find_parent_fdesc(msg, attrs):
    """ locate this field among its parents fields and return the
        field descriptor
    """

    parent = _find_field(msg, attrs[:-1])
    parent_desc = parent.DESCRIPTOR
    for fdesc in parent_desc.fields:
        # If we matched the last element, then we found it
        if (fdesc.name == attrs[-1]): return (fdesc)
    
    return (None)


def _get_topo(msg):
    """ get the topology for a GPB message """
    topo = {}
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:

        field = getattr(msg, fdesc.name)

        isRepeated = (not (getattr(field, '_values', None) is None))
        if fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE:
            if (not isRepeated): # not repeated
                the_topo = _get_topo(field)
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
            the_topo = typename.get(fdesc.cpp_type, 'unknown')

        if (not isRepeated): # not repeated
            topo[fdesc.full_name] = the_topo
        else:
            topo[fdesc.full_name] = [the_topo]

    return topo


def _get_repeats(msg, repeats=[ ], base_name=''):
    """ build a list of the repeated items for a GPB message """
    '''
    msg.DESCRPIPTOR.fields
    [{
        name
        full_name (starts with package.publication)
        cpp_type
        message_type.name (if repeating)
    }]
    msg.field1
    {
        _values[ ] (if repeating)
        subfield1
        ...
        subfieldN
    }
    ...
    msg.fieldN
    msg.message_type_name (if repeating)
    '''
    
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:

        field = getattr(msg, fdesc.name)

        if fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE:
            # Only structured values might have repeating elements (or sub-elements)
            repeated_name = "%s.%s" %(base_name, fdesc.name) if (base_name) else fdesc.name
            if (not (getattr(field, '_values', None) is None)): # repeated element
                if (not repeated_name in repeats): repeats.append(repeated_name)
                field = getattr(msg, fdesc.message_type.name)
            # Look for child repeats
            _get_repeats(field, repeats, repeated_name)
        else:
            # There are no repeats in scalar values, nor trees to descend
            pass

    return (repeats)  # Only the last return will be picked up


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

        field = getattr(msg, fdesc.name)
        if getattr(field, '_values', None) is None: # not repeated
            if fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE:
                if fdesc.message_type.name == 'info_header':
                    tokens.append(str(field.info_generation_time_ts_utc))
                    tokens.append(str(field.info_generation_time_ts_lct))
                    tokens.append('<header>')
                else:
                    tokens.extend(_extract_msg(field))
            else: # simple types
                tokens.append(str(field))

        else: # repeated field
            tokens.append(str(len(field)))
            for subfield in field:
                if fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE:
                    tokens.extend(_extract_msg(subfield))
                else:
                    tokens.append(str(subfield))

    return tokens


def _get_msg_dict(msg):
    """ get the value for a message as a dict """
    msg_dict = {}
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:
        field = getattr(msg, fdesc.name)

        if getattr(field, '_values', None) is None: # not repeated
            if fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE:
                msg_dict[fdesc.name] = _get_msg_dict(field)
            else:
                msg_dict[fdesc.name] = field
        else:# repeated
            if fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE:
                msg_dict[fdesc.name] = [ _get_msg_dict(fd) for fd in field ]
            else:
                msg_dict[fdesc.name] = getattr(field, '_values')

    return msg_dict


def _flatten(msg, prefix='', exist_fields={}):
    """ get the flatten for a message as a dict or a list of dict"""

    if prefix != '': prefix += '.'

    result = exist_fields.copy()
    desc = msg.DESCRIPTOR
    for fdesc in desc.fields:
        fd_name, fd_type = fdesc.name, fdesc.cpp_type
        field = getattr(msg, fd_name)

        if getattr(field, '_values', None): #repeated
            #we only support one repeated field, return immediately
            return [_flatten(fd, prefix + fd_name, result) for fd in field]
        else: # not repeated
            if fd_type == fdesc.CPPTYPE_MESSAGE:
                t_result = _flatten(field, prefix + fd_name) 
                if isinstance(t_result, dict):
                    result.update(t_result)
                else:
                    return [dict(result, **d) for d in t_result]
            else:
                result[prefix + fd_name] = field

    return result


class GpbMessage(object):
    """ A wrapper class for GPB Message"""

    #instance properties and methods
    __slots__ = ( '__pkg_name', '__msg_name', '__rawstr', '__msg' )

    def __init__( self, pkg_name, msg_name ):
        """ constructor """
        self.__pkg_name = pkg_name
        self.__msg_name = msg_name
        self.__msg      = None
        self.__rawstr   = None
        self._import()


    def __repr__(self):
        """ to representation """
        return self.__msg.__str__()


    def _import(self):
        """ import the message class, and create an instance in self.__msg """
        module = __import__('%s.%s_pb2' % (self.__pkg_name, self.__msg_name),
                            globals(), locals(), [self.__msg_name])

        msg_class = getattr(module, self.__msg_name)

        self.__msg = msg_class()
        return


    def get_field(self, field_name):
        """ get the field msg, according to the field_name """

        field_names = field_name.split('.')
        return _find_field(self.__msg, field_names)


    def field_value(self, field_name):
        """ get a value from the GPB message """

        return self.get_field(field_name)


    def clear_field(self, field_name):
        """ clear the field """
        field_names = field_name.split('.')
        if len(field_names) == 1:
            self.__msg.ClearField(field_name)
        else:
            field = self.get_field('.'.join(field_names[:-1]))
            field.ClearField(field_name[-1])
        return


    def set_field_value(self, field_name, value):
        """ set the value to GPB message """
        field_names = field_name.split('.')

        # find the parent field
        if len(field_names) == 1:
            field = self.__msg
        else:
            field = self.get_field('.'.join(field_names[:-1]))

        setattr(field, field_names[-1], value)
        return


    def add_field_values(self, field_name, values):
        """ add the value list to GPB message, only work for simple variables
        """
        # we need the actual field
        field = self.get_field(field_name)

        field.extend(values)
        return
    
    def get_repeated_fields(self, field_name):
        """ return the collection of repeated values for the given field """
        
        values = None
        repeats = self.get_repeats( )
        if (field_name in repeats):
            # field_name is a repeating value, so build a list of dictionaries with values
            repeated_names = field_name.split('.')
            repeated_tree = _find_field(self.__msg, repeated_names)
            fdesc = _find_parent_fdesc(self.__msg, repeated_names)
            # Now get the list of repeating element values
            values = [ ]
            for subfield in repeated_tree:
                if (fdesc.cpp_type == fdesc.CPPTYPE_MESSAGE):
                    ## TODO: Eliminate items not part of the message???
                    values.append(_get_msg_dict(subfield))
                else:
                    values.append(subfield)
            
        return values

    def loadtabs(self, astring, component_id):
        """ feed by a TAB splitted string """

        import Queue
        tokens = Queue.deque(astring.replace('\\t', '\t').split('\t'))

        _fill_msg(self.__msg, tokens, component_id)
        return


    def loadjson(self, json_str):
        adict = json.loads(json_str)
        _fill_from_json(self.__msg, adict)
        return


    def loads(self, astring):
        """ wrapper for GPB's ParseFromString """
        self.__rawstr = astring
        self.__msg.ParseFromString(astring)
        return


    def dumps(self):
        """ wrapper for GPB's SerializedToString """
        return self.__msg.SerializeToString()


    def topo(self):
        """ get the topology for the GPB message, return a dict """
        return _get_topo(self.__msg)


    def dumpjson(self):
        """ return a dict containing all values """
        adict = _get_msg_dict(self.__msg)
        return json.dumps(adict)


    def dumptabs(self):
        """ return a string send to tpa_publish """
        tokens = _extract_msg(self.__msg)
        return '\t'.join(tokens)


    def flatten(self):
        """ return a dict or list contain the flatten of the msg """
        return _flatten(self.__msg)


    def get_repeats(self):
        """ return a list of the fields that repeat """
        return _get_repeats(self.__msg, repeats=[ ])


    @property
    def rawstr(self):
        """ return the raw string """
        return self.__rawstr

    @property
    def message_name(self):
        """ return the message name """
        return '%s.%s' % (self.__pkg_name, self.__msg_name)


    @property
    def msg(self):
        """ return the self.__msg """
        return self.__msg


if __name__ == '__main__':
    print 'Gpbmessage'

