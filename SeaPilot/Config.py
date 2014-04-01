"""
   @@@ START COPYRIGHT @@@

   (C) Copyright 2013-2014 Hewlett-Packard Development Company, L.P.

   @@@ END COPYRIGHT @@@

   provide the configuration information specific for SeaPilot

   $Id: Logger.py 38507 2014-03-19 04:37:49Z cheng-xin.cai $

"""

import os
import socket
from subprocess import Popen, PIPE, STDOUT

class Config(object):
    """
       The Seapilot configuration information
    """
    __cluster_id   = None
    __domain_id    = None
    __subdomain_id = None
    __instance_id  = None

    __ip_address = '127.0.0.1'
    __host_id    = 0

    def __init__(self):
        hostname = socket.getfqdn()
        self.__ip_address = socket.gethostbyname(hostname)

        proc = Popen(['/usr/bin/hostid'], stdout=PIPE, stderr=STDOUT)
        out, _err = proc.communicate()

        self.__host_id = int(out.split('\n')[0], 16)

        self.__cluster_id   = os.getenv('SEAPILOT_CLUSTER_ID', 0)
        self.__domain_id    = os.getenv('SEAPILOT_DOMAIN_ID', 0)
        self.__subdomain_id = 0
        self.__instance_id  = os.getenv('SEAPILOT_INSTANCE_ID', 0)

    @property
    def ip_address(self):
        return self.__ip_address

    @property
    def host_id(self):
        return self.__host_id

    @property
    def cluster_id(self):
        return self.__cluster_id

    @property
    def domain_id(self):
        return self.__domain_id

    @property
    def subdomain_id(self):
        return self.__subdomain_id

    @property
    def instance_id(self):
        return self.__instance_id
