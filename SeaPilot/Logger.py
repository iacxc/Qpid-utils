"""

   provide the common logger specific for SeaPilot

"""

__all__ = ( 'Logger' )

import os, sys
import logging
from logging.handlers import RotatingFileHandler


class Logger(object):
    """
       a wrap of logging.Logger, which has a default location and file name
    """
    def __init__(self, name_=None, maxbypes_=10*1024*1024, backupcount_=3):
        self.__logger = logging.getLogger(name_)

        log_file = default_log_loc()
        log_path = os.path.dirname(log_file)
        if not os.path.isdir(log_path):
            os.makedirs(log_path)

        formatter = logging.Formatter('[%(asctime)s (%(process)d)] ' \
                                           '%(levelname)s: %(message)s')

        self.__handler_f = RotatingFileHandler(log_file,
                          maxBytes=maxbypes_, backupCount=backupcount_)
        self.__handler_f.setFormatter(formatter)

        self.__handler_s = logging.StreamHandler(sys.stdout)
        self.__handler_s.setFormatter(formatter)

        self.__logger.addHandler(self.__handler_f)


    def set_verbose(self, verbose_=True):
        """ turn on/off verbose """
        if verbose_:
            self.__logger.addHandler(self.__handler_s)
        else:
            self.__logger.removeHandler(self.__handler_s)


    def set_loglevel(self, level_):
        """ wrapper for logging.Logger.setLevel """
        self.__logger.setLevel(level_)


    def __getattr__(self, *args, **kws):
        """ pass all unknown attributes to __logger """
        return getattr(self.__logger, *args, **kws)


def default_log_loc():
    """ get the default file location and name for the log """
    return '{0}/logs/{1}.log'.format(
        os.getenv('SEAPILOT_HOME', ''), os.path.basename(sys.argv[0]))

