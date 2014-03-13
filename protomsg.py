
# $Id: protomsg.py 290 2013-07-28 11:48:00Z chengxin.cai $

""" Provide all needed functions, classes for handle google protocol buffer
"""

import os, sys
import time
import socket
import thread

__all__ = ('GpbMessage')

def init_qpid_header(qpid_header, component_id):
    """ initialize a common.qpid_header, the input parameter qpid_header
        must be a qpid_header instance
    """

    # set the fields of qpid_header one by one
    time_utc = time.time()
    time_lct = time_utc - time.altzone

    qpid_header.generation_time_ts_utc = int(time_utc * 1e6)   #1
    qpid_header.generation_time_ts_lct = int(time_lct * 1e6)   #2
    qpid_header.version      = 1                               #3
    qpid_header.cluster_id   = 0                               #4
    qpid_header.domain_id    = 0                               #5
    qpid_header.subdomain_id = 0                               #6
    qpid_header.instance_id  = 0                               #7
    qpid_header.tenant_id    = 0                               #8
    qpid_header.component_id = component_id                    #9
    qpid_header.process_id   = os.getpid()                     #10
    try:
        qpid_header.thread_id    = abs(thread.get_ident())     #11
    except ValueError: # a temp fix for 32 bit proto files
        qpid_header.thread_id    = abs(thread.get_ident()) % 1024  #11

    qpid_header.node_id      = 0                               #12
    qpid_header.pnid_id      = 0                               #13

    hostname = socket.getfqdn()
    qpid_header.ip_address_id = socket.gethostbyname(hostname) #14

    qpid_header.sequence_num = 10                              #15
    qpid_header.process_name = os.path.basename(sys.argv[0])   #16

    nums = map(int, qpid_header.ip_address_id.split('.'))
    qpid_header.host_id      = nums[1] << 24 | nums[0] << 16 \
                             | nums[3] << 8  | nums[2]         #17

    qpid_header.system_version = ''                            #18

def init_info_header(info_header, component_id = 13):
    """ initialize a common.info_header, the input parameter qpid_header
        must be a qpid_header instance
    """

    header = info_header.header
    init_qpid_header(header, component_id)                     #1

    info_header.info_generation_time_ts_utc = header.generation_time_ts_utc #2
    info_header.info_generation_time_ts_lct = header.generation_time_ts_lct #3
    info_header.info_version      = header.version             #4
    info_header.info_cluster_id   = header.cluster_id          #5
    info_header.info_domain_id    = header.domain_id           #6
    info_header.info_subdomain_id = header.subdomain_id        #7
    info_header.info_instance_id  = header.instance_id         #8
    info_header.info_tenant_id    = header.tenant_id           #9
    info_header.info_component_id = header.component_id        #10
    info_header.info_process_id   = header.process_id          #11
    info_header.info_thread_id    = header.thread_id           #12

    info_header.info_node_id      = header.node_id             #13
    info_header.info_pnid_id      = header.pnid_id             #14

    info_header.info_ip_address_id = header.ip_address_id      #15
    info_header.info_sequence_num  = header.sequence_num       #16
    info_header.info_process_name  = header.process_name       #17
    info_header.info_host_id       = header.host_id            #18

#
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

def _fill_msg(msg, tokens):
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
                        init_info_header(msg.header)
                    else:
                        tokens.appendleft(token)  # push back the token
                        _fill_msg(field, tokens)
                else: # simple types
                    try:
                        setattr(msg, fd_name, converter.get(fd_type, str)(token))
                    except ValueError, e:
                        print e
                        print 'Error in', fd_name

            else: # repeated field
                if fd_type == fdesc.CPPTYPE_MESSAGE:
                    for _ in range(int(token)):
                        _fill_msg(field.add(), tokens)

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
        sys.path.insert(0, self.__proto_path)
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

    def loadtabs(self, astring):
        """ feed by a TAB splitted string """
        import Queue
        tokens = Queue.deque(astring.replace('\\t', '\t').split('\t'))

        _fill_msg(self.__msg, tokens)

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

    import json

    proto_path = os.path.join(os.environ['HOME'], 'protos')

    pm1 = GpbMessage(proto_path, 'ndcs', 'query_start_stats')
    print '%s %s' % (pm1.message_name, json.dumps(pm1.topo(), indent=2))

    tpaString = '\t'.join([
        '<header>',                # 1
        'MXID11000031366212240958796138621000000001606U6553400',    # 2
        'SQL_CUR_2',               # 3
        'MXID11000031366212240958796138621000000001606U6553400_269_SQL_CUR_2',
        'SQL_EXE_UTIL',            # 5
        'ccai',                    # 6
        'SQLUSER_ADMIN',           # 7
        '65534',                   # 8
        'DB__USERADMIN',           # 9
        'HPDCI',                   # 10
        '\NSK',                    # 11
        '0,31366',                 # 12
        'Admin_Load_DataSource',   # 13
        'HP_DEFAULT_SERVICE',      # 14
        '1374212512432376',        # 15
        '1374212512432376',        # 16
        '1374212512435401',        # 17
        '1374212512435401',        # 18
        '0',                       # 19
        '1374212512423938',        # 20
        '1374212512423938',        # 21
        '1374212512432139',        # 22
        '1374212512432139',        # 23
        '8201',                    # 24
        '0.0',                     # 25
        '1.0',                     # 26
        '0.0',                     # 27
        '0.0',                     # 28
        '0.0',                     # 29
        '0.0',                     # 30
        '0.0',                     # 31
        '0.0',                     # 32
        '0',                       # 33
        '1449043952',              # 34
        '0',                       # 35
        '0',                       # 36
        '0',                       # 37
        '0',                       # 38
        '0',                       # 39
        '0',                       # 40
        '0',                       # 41
        '0.0',                     # 42
        '0.0',                     # 43
        '0.0',                     # 44
        '1374212512433344',        # 45
        '1374212512433344',        # 46
        'INIT',                    # 47
        'N/A',                     # 48
        '0',                       # 49
        '0',                       # 50
        'NONE',                    # 51
        'get tables',              # 52
        '0',                       # 53
        '212240958830832254000372000',  # 54
        '10000',                   # 55
        '0',                       # 56
        '0',                       # 57
        '0',                       # 58
        '0',                       # 59
        '0',                       # 60
        '0',                       # 61
        '0',                       # 62
        '2',                       # 63
        '0',                       # 64
        '1',                       # 65
        '278312',                  # 66
        '809032',                  # 67
        '24',                      # 68
        '3502',                    # 69
        '0',                       # 70
        'NO',                      # 71
        '<N/A>',                   # 72
        '<N/A>',                   # 73
        '<N/A>',                   # 74
        '0.0',                     # 75
        '0.0',                     # 76
        '0',                       # 77
        'DISK',                    # 78
        '0'])                      # 79
    pm1.loadtabs(tpaString)

    if tpaString != pm1.dumptabs():
        raise AssertError('dumptabs')
    print '%s\n', pm1

    testvec = ['simple1', 'simple2', 'rep1', 'rep2']
    msgs = dict( (msgname, GpbMessage(proto_path, 'protest', msgname))
                   for msgname in testvec )

    print 'Show Topos:'
    for pmsg in msgs.itervalues():
        print '%s %s' % (pmsg.message_name, json.dumps(pmsg.topo(), indent=4))

    print '\nShow Empty message:'
    for pmsg in msgs.itervalues():
        print '%s %s' % (pmsg.message_name, json.dumps(pmsg.dumpjson(), indent=4))

    # fill messages
    msgs['simple1'].set_field_value('id', 1)
    msgs['simple1'].set_field_value('name', 'simple1')

    msgs['simple2'].loadjson(
        {"body": {"id": 10, "name": "simple2"},"id": 2})

    msgs['rep1'].loadjson(
        { "name": [ "name1", "name2", "name3" ], "id": 1})

    msgs['rep2'].loadtabs('2\t3\t10\tName1\t20\tName2\t30\tName3')

    print '\nShow Message with values:'
    for pmsg in msgs.itervalues():
        print '%s: %s' % (pmsg.message_name,
                          json.dumps(pmsg.dumps(), indent=4))

    print '\nShow Message as tpa input:'
    for pmsg in msgs.itervalues():
        print '%s: %s' % (pmsg.message_name,
                          pmsg.dumptabs().replace('\t','<TAB>'))

    print '\nShow Message in raw:'
    for pmsg in msgs.itervalues():
        print '%s:\n{ %s}' % (pmsg.message_name,
                             str(pmsg).replace('\n', ' '))

    print '\nShow field body:'
    print [ '{ %s}' % str(v).replace('\n', ' ')
            for v in msgs['rep2'].field_value('body') ]

    print '\nShow serialize to string:'
    print msgs['rep2'].dumps()

