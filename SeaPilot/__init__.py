"""
   @@@ START COPYRIGHT @@@

   (C) Copyright 2013-2014 Hewlett-Packard Development Company, L.P.

   @@@ END COPYRIGHT @@@

   __init__.py for SeaPilot

   $Id: __init__.py 38647 2014-03-24 07:42:55Z cheng-xin.cai $

"""


import os


def default_context_path(check_name_):
    """
        Return the default context file path
        Notice the default path is not in /home
    """
    return "{0}/var/checks/context/{1}/automatic".format(
                   os.getenv('SEAPILOT_HOME', ''), check_name_)


