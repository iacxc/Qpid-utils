"""

   __init__.py for SeaPilot

"""

__all__ = ( 'default_context_path' )

import os


def default_context_path(check_name_):
    """
        Return the default context file path for a specific check
        Notice the default path is not in /home
    """
    return "{0}/var/checks/context/{1}/automatic".format(
                   os.getenv('SEAPILOT_HOME', ''), check_name_)


