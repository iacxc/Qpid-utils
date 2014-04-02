"""
   __init__.py for Amqp

   $Id: __init__.py 38908 2014-04-01 03:22:04Z cheng-xin.cai $

"""


import os
import sys
import socket
import thread

import Common
from SeaPilot.Config import Config

def next_sequence_num(buf=[0]): # use list as default value to make sure it is
                                # initialized only once
    """
       get the next sequence number
    """
    val = buf[0]
    buf[0] += 1
    return val

__config = Config()

def init_qpid_header(header, component_id,
                           cluster_id=__config.cluster_id,
                           domain_id=__config.domain_id,
                           subdomain_id=__config.subdomain_id,
                           instance_id=__config.instance_id):
    """ Init the qpid_header """
    assert header.__class__.__name__ == 'qpid_header'

    ts_utc, ts_lct = Common.current_timestamps()
    header.generation_time_ts_utc = ts_utc
    header.generation_time_ts_lct = ts_lct
    header.version                = 1
    header.cluster_id             = cluster_id
    header.domain_id              = domain_id
    header.domain_id              = domain_id
    header.subdomain_id           = subdomain_id
    header.instance_id            = instance_id
    header.tenant_id              = 0
    header.component_id           = component_id
    header.process_id             = os.getpid()
    header.thread_id              = thread.get_ident() & 0xFFFF

    header.ip_address_id = __config.ip_address

    header.sequence_num = next_sequence_num()

    header.host_id = __config.host_id


def init_info_header(header, component_id,
                           cluster_id=__config.cluster_id,
                           domain_id=__config.domain_id,
                           subdomain_id=__config.subdomain_id,
                           instance_id=__config.instance_id):
    """ Init the info_header """
    assert header.__class__.__name__ == 'info_header'

    init_qpid_header(header.header, component_id, cluster_id, domain_id,
                   subdomain_id, instance_id)
    qpid_header = header.header

    header.info_generation_time_ts_utc = qpid_header.generation_time_ts_utc
    header.info_generation_time_ts_lct = qpid_header.generation_time_ts_lct
    header.info_version                = qpid_header.version
    header.info_cluster_id             = qpid_header.cluster_id
    header.info_domain_id              = qpid_header.domain_id
    header.info_domain_id              = qpid_header.domain_id
    header.info_subdomain_id           = qpid_header.subdomain_id
    header.info_instance_id            = qpid_header.instance_id
    header.info_tenant_id              = qpid_header.tenant_id
    header.info_component_id           = qpid_header.component_id
    header.info_process_id             = qpid_header.process_id
    header.info_thread_id              = qpid_header.thread_id
    header.info_sequence_num           = qpid_header.sequence_num
    header.info_ip_address_id          = qpid_header.ip_address_id
    header.info_host_id                = qpid_header.host_id

def init_event_header(header, component_id, event_id, event_severity,
                           cluster_id=__config.cluster_id,
                           domain_id=__config.domain_id,
                           subdomain_id=__config.subdomain_id,
                           instance_id=__config.instance_id):
    """ Init the event_header """
    assert header.__class__.__name__ == 'event_header'

    init_info_header(header.header, component_id, cluster_id, domain_id,
                   subdomain_id, instance_id)
    header.event_id = event_id
    header.event_severity = event_severity


def init_herf_header(header, component_id,
                           requested_sampling_interval=0,
                           actual_sampling_interval=0,
                           cluster_id=__config.cluster_id,
                           domain_id=__config.domain_id,
                           subdomain_id=__config.subdomain_id,
                           instance_id=__config.instance_id):
    """ Init the perf_header """
    assert header.__class__.__name__ == 'perf_header'

    init_info_header(header.header, component_id, cluster_id, domain_id,
                   subdomain_id, instance_id)
    header.requested_sampling_interval_ms = requested_sampling_interval
    header.actual_sampling_interval_ms = actual_sampling_interval


def init_health_header(header, component_id, publication_type,
                           check_interval=0, error=0, error_text='',
                           cluster_id=__config.cluster_id,
                           domain_id=__config.domain_id,
                           subdomain_id=__config.subdomain_id,
                           instance_id=__config.instance_id):
    """ Init the health_header """
    assert header.__class__.__name__ == 'health_header'

    init_info_header(header.header, component_id, cluster_id, domain_id,
                   subdomain_id, instance_id)
    header.publication_type = publication_type
    header.check_interval_ms = check_interval
    header.error = error
    header.error_text = error_text


def set_qpid_header_sequence_number(header):
    """ set sequence number for qpid_header """
    assert header.__class__.__name__ == 'qpid_header'

    header.sequence_num = next_sequence_num()


def set_info_header_sequence_number(header):
    """ set sequence number for info_header and qpid_header """
    assert header.__class__.__name__ == 'info_header'

    set_qpid_header_sequence_number(header.header)
    header.info_sequence_num = header.header.sequence_num


def set_amqp_header_service_identifier (header, process_name):
    """ set the process_name for qpid_header and info_header"""
    assert header.__class__.__name__ == 'info_header'

    header.header.process_name = process_name
    header.info_process_name = process_name


def set_proto_src(path):
    """ set the publications for google protocol buffer """
    if sys.path.count(path) == 0:
        sys.path.append(path)

