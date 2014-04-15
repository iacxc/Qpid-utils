"""

    This module provide a simple implementation of RoutingKey class

"""


__all__ = ( 'RoutingKey',
            'TooManyComopnents', 
            'TooManyFreeString', 
            'RepteatedComponent',
          )

import Common

#enumations for publication fields
SP_CATEGORY = Common.make_enum('SP_CATEGORY',
        NULL         = 'null',
        EVENT        = 'event',
        PERF_STAT    = 'performance_stat',
        HEALTH_STATE = 'health_state',
        SECURITY     = 'security')

SP_PACKAGE = Common.make_enum('SP_PACKAGE',
        NULL                         = 'null',
        COMMON                       = 'common',
        DTM                          = 'dtm',
        INCIDENTANALYSIS             = 'incidentanalysis',
        LIGHTHOUSE                   = 'lighthouse',
        MONITOR                      = 'monitor',
        NDCS                         = 'ndcs',
        SE                           = 'se',
        SUMMARY                      = 'summary',
        UNC                          = 'unc',
        SQL                          = 'sql',
        SPACE                        = 'space',
        NVTSVC                       = 'nvtsvc',
        PROBLEMMANAGEMENT            = 'problem_management',
        LUN                          = 'lun',
        SSD                          = 'ssd',
        MANAGEABILITY_INFRASTRUCTURE = 'manageability_infrastructure',
        STATE                        = 'state',
        MANAGEABILITY_TEST           = 'manageability_test',
        WMS                          = 'wms',
        TPG                          = 'tpg',
        CHAMELEON                    = 'chameleon',
        ACTION                       = 'action',
        OS                           = 'os',
        LINUXCOUNTERS                = 'linuxcounters',
        ACCESSLAYER                  = 'accesslayer',
        DATABASELAYER                = 'databaselayer',
        FOUNDATIONLAYER              = 'foundationlayer',
        OSLAYER                      = 'oslayer',
        SERVERLAYER                  = 'serverlayer',
        STORAGELAYER                 = 'storagelayer',
        INFRASTRUCTURELAYER          = 'infrastructurelayer',
        DBS                          = 'dbs',
        COMPRESSION                  = 'compression',
        HADOOPCOUNTERS               = 'hadoopcounters',
        DSINORMALIZEDCOUNTERS        = 'dsinormalizedcounters',
        TNA                          = 'tna',
        LOGDIST                      = 'logdist',
        VERTICA                      = 'vertica',
        MAPR                         = 'mapr',
        SEAQUEST                     = 'seaquest',
        MYSQL                        = 'mysql')

SP_SCOPE = Common.make_enum('SP_SCOPE',
        NULL     = 'null',
        CLUSTER  = 'cluster',
        INSTANCE = 'instance',
        TENANT   = 'tenant')

SP_SECURITY = Common.make_enum('SP_SECURITY',
        NULL    = 'null',
        PUBLIC  = 'public',
        PRIVATE = 'private')

SP_PROTOCOL = Common.make_enum('SP_PROTOCOL',
        NULL = 'null',
        GPB  = 'gpb',
        XML  = 'xml')

SP_PUBLICATION = Common.make_enum('SP_PUBLICATION',
        NULL        = 'null')

SP_CATEGORY_SET = set(SP_CATEGORY.enums.values())
SP_PACKAGE_SET  = set(SP_PACKAGE.enums.values())
SP_SCOPE_SET    = set(SP_SCOPE.enums.values())
SP_SECURITY_SET = set(SP_SECURITY.enums.values())
SP_PROTOCOL_SET = set(SP_PROTOCOL.enums.values())


class TooManyComopnents(Exception):
    """ Exception for too many components """
    pass


class TooManyFreeString(Exception):
    """ Exception for too many free strings """
    pass


class RepteatedComponent(Exception):
    """ Exception for too repeated component """
    pass


class RoutingKey(object):
    """ the routing key class """
    def __init__(self, key=None):
        self.__category    = SP_CATEGORY.NULL
        self.__package     = SP_PACKAGE.NULL
        self.__scope       = SP_SCOPE.NULL
        self.__security    = SP_SECURITY.NULL
        self.__protocol    = SP_PROTOCOL.NULL
        self.__publication = SP_PUBLICATION.NULL

        if key:
            self.set_from_string(key)


    def set_from_string(self, key_str):
        """ set all the fields from a string """
        for field in key_str.split('.'):
            if field in SP_CATEGORY_SET:
                #category
                if self.__category == SP_CATEGORY.NULL:
                    self.__category = field
                else:
                    raise RepteatedComponent('category:{0}'.format(field))

            elif field in SP_PACKAGE_SET:
                #package
                if self.__package == SP_PACKAGE.NULL:
                    self.__package = field
                else:
                    raise RepteatedComponent('package:{0}'.format(field))

            elif field in SP_SCOPE_SET:
                #scope
                if self.__scope == SP_SCOPE.NULL:
                    self.__scope = field
                else:
                    raise RepteatedComponent('scope:{0}'.format(field))

            elif field in SP_SECURITY_SET:
                #security
                if self.__security == SP_SECURITY.NULL:
                    self.__security = field
                else:
                    raise RepteatedComponent('security:{0}'.format(field))

            elif field in SP_PROTOCOL_SET:
                #protocol
                if self.__protocol == SP_PROTOCOL.NULL:
                    self.__protocol = field
                else:
                    raise RepteatedComponent('protocol:{0}'.format(field))

            else:
                #publication
                if self.__publication == SP_PUBLICATION.NULL:
                    self.__publication = field
                else:
                    raise TooManyFreeString('{0}'.format(field))



    # properties
    @property
    def category(self):
        """ category """
        return self.__category

    @property
    def package(self):
        """ package """
        return self.__package

    @property
    def scope(self):
        """ scope """
        return self.__scope

    @property
    def security(self):
        """ security """
        return self.__security

    @property
    def protocol(self):
        """ protocol """
        return self.__protocol

    @property
    def publication(self):
        """ publication """
        return self.__publication

    @property
    def message_name(self):
        """ message name """
        return '.'.join([self.__package, self.__publication])

    @property
    def subscribe_str(self):
        """ a string to subscribe """
        fields = []
        null_number = 0

        for val in (self.__category, self.__package, self.__scope,
                    self.__security, self.__protocol, self.__publication):

            if val == 'null':
                null_number += 1
                continue

            if null_number == 1:
                fields.append('*')
            elif null_number > 1:
                fields.append('#')

            fields.append(val)
            null_number = 0

        # for the last field
        if null_number == 1:
            fields.append('*')
        elif null_number > 1:
            fields.append('#')

        return '.'.join(fields)


    def __repr__(self):
        return '.'.join([self.__category, self.__package, self.__scope,
                       self.__security, self.__protocol, self.__publication])


if __name__ == '__main__':
    print 'RoutingKey.py'

