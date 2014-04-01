"""
   @@@ START COPYRIGHT @@@

   (C) Copyright 2013-2014 Hewlett-Packard Development Company, L.P.

   @@@ END COPYRIGHT @@@

   $Id: Common.py 38587 2014-03-21 01:20:25Z cheng-xin.cai $

   provide some common utilities
"""

__all__ = []

import os
from subprocess import Popen, PIPE, STDOUT
import threading


#constants
SUCCESS = 0
FAILURE = 1

E_TIMEOUT = -124
E_NONEXE  = -126
E_NONEXT  = -127

TPA_PUBLISH = '{0}/bin/TPA_Publish'.format(os.getenv('SEAPILOT_HOME'))


class Command(object):
    """
       a class to implement running a command with timeout
    """
    cmdstr = None
    process = None
    status = None
    output, error = '', ''

    def __init__(self, cmdstr):
        self.cmdstr = cmdstr

    def run(self, timeout=None):
        """ Run a command then return: (status, output + error). """
        def target():
            """ a closure which run the command"""
            try:
                self.process = Popen(self.cmdstr, shell=True,
                                                  stdout=PIPE, stderr=STDOUT)
                self.output, self.error = self.process.communicate()
                self.status = self.process.returncode
            except Exception, e:
                self.status = e.errno
                self.error = e.strerror

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            self.process.terminate()
            thread.join()
            self.status = E_TIMEOUT
            self.error = "TIMEOUT"

        return self.status, self.output


def run_cmd(cmdstr, timeout=30):
    """
       a wrapper to call Command.run, it performs some checks before invoke
       the command
    """
    cmd = cmdstr.split(' ')[0]
    if cmd.startswith('/'):
        if not os.path.exists(cmd):
            return E_NONEXT, '{0}: NO SUCH FILE OR DIRECTORY'.format(cmd)

        elif not os.access(cmd, os.X_OK):
            return E_NONEXE, '{0}: PERMISSION DENIED'.format(cmd)

    command = Command(cmdstr)
    status, output = command.run(timeout)

    return status, output


def make_enum(enum_type='Enum', base_classes=None, methods=None, **attrs):
    """
    Generates a enumeration with the given attributes.
    """
    # Enumerations can not be initalized as a new instance
    def __init__(_instance, *_args, **_kws):
        raise RuntimeError('%s types can not be initialized.' % enum_type)

    if base_classes is None:
        base_classes = ()

    if methods is None:
        methods = {}

    base_classes = base_classes + (object,)
    for key, val in methods.iteritems():
        methods[key] = classmethod(val)

    attrs['enums'] = attrs.copy()
    methods.update(attrs)
    methods['__init__'] = __init__
    return type(enum_type, base_classes, methods)


def current_timestamps():
    """
       get the current timestamp, utc and lct, in microseconds
    """
    import time
    utc_usecs = time.time() * 1000000

    lct_usecs = utc_usecs - (time.altzone + 3600 * time.daylight) * 1000000

    return (int(utc_usecs), int(lct_usecs))


def current_node_name():
    """
       get the hostname
    """
    import socket
    return socket.getfqdn()

