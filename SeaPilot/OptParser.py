"""
   @@@ START COPYRIGHT @@@

   (C) Copyright 2013-2014 Hewlett-Packard Development Company, L.P.

   @@@ END COPYRIGHT @@@

   provide an option parser specific for SeaPilot

   $Id: OptParser.py 38507 2014-03-19 04:37:49Z cheng-xin.cai $

"""


import optparse


class OptParser(object):
    """
       a wrapper of optparse.OptionParser, has some common option
       for SeaPilot processes, like --verbose and --debug
    """
    def __init__(self, *args, **kws):
        parser = optparse.OptionParser(*args, **kws)

        parser.add_option('-v', '--verbose', dest='verbose',
               action='store_true', default=False,
               help='whether display the output to screen')
        parser.add_option('-d', '--debug', dest='debug',
               action='store_true', default=False,
               help='whether display the debug information to output')

        self.__parser = parser


    def __getattr__(self, *args, **kws):
        return getattr(self.__parser, *args, **kws)

