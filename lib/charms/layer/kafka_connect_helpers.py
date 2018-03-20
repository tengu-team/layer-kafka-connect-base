import json
import requests
import collections
from charms.reactive import clear_flag,set_flag
from charmhelpers.core import unitdata
from charmhelpers.core.hookenv import log


# General functions


def set_worker_config(config):
    '''List with available configs
    https://docs.confluent.io/current/connect/allconfigs.html#connect-allconfigs
    '''
    unitdata.kv().set('worker.properties', config)
    clear_flag('kafka-connect-base.configured')


def set_base_image(image):
    unitdata.kv().set('docker-image', image)
    clear_flag('kafka-connect-base.configured')


def get_worker_service():
    return unitdata.kv().get('kafka-connect-service', '')


def get_configs_topic():
    return unitdata.kv().get('connectconfigs', None)

def get_offsets_topic():
    return unitdata.kv().get('connectoffsets', None)

def get_status_topic():
    return unitdata.kv().get('connectstatus', None)


# The following functions perform REST api calls to the workers. 
#
# They all return a named tuple Api_response with the following format:
#     Api_response(status_code, json)
# If a RequestException is raised then None is returned.
#
# A list with available functions:
#     - register_connector(connector, connector_name)
#     - unregister_connector(connector_name)
#     - list_connectors()
#     - connector_status(connector_name)
#     - connector_restart(connector_name)
#     - connector_pause(connector_name)
#     - connector_resume(connector_name)
#     - list_tasks(connector_name)


Api_response = collections.namedtuple('Api_response', ['status_code', 'json'])

def register_connector(connector, connector_name):
    all_connectors = unitdata.kv().get('connectors', {})
    all_connectors[connector_name] = connector
    unitdata.kv().set('connectors', all_connectors)
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.put("http://" + get_worker_service() + "/connectors/" + connector_name + '/config',
                        json=connector,
                        headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None


def unregister_connector(connector_name):
    all_connectors = unitdata.kv().get('connectors', {})
    all_connectors.pop(connector_name, None)
    unitdata.kv().set('connectors', all_connectors)
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.delete("http://" 
                            + get_worker_service()
                            + "/connectors/"
                            + connector_name,
                            headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None
    except json.decoder.JSONDecodeError:
        return Api_response(r.status_code, None)


def list_connectors():
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.get("http://" + get_worker_service() + '/connectors',
                         headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None


def connector_status(connector_name):
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.get("http://" 
                         + get_worker_service()
                         + '/connectors/'
                         + connector_name
                         + '/status',
                         headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None


def connector_restart(connector_name):
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.post("http://" 
                         + get_worker_service()
                         + '/connectors/'
                         + connector_name
                         + '/restart',
                         headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None
    except json.decoder.JSONDecodeError:
        return Api_response(r.status_code, None)


def connector_pause(connector_name):
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.put("http://" 
                         + get_worker_service()
                         + '/connectors/'
                         + connector_name
                         + '/pause',
                         headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None
    except json.decoder.JSONDecodeError:
        return Api_response(r.status_code, None)


def connector_resume(connector_name):
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.put("http://" 
                         + get_worker_service()
                         + '/connectors/'
                         + connector_name
                         + '/resume',
                         headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None
    except json.decoder.JSONDecodeError:
        return Api_response(r.status_code, None)


def list_tasks(connector_name):
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.get("http://" 
                         + get_worker_service()
                         + '/connectors/'
                         + connector_name
                         + '/tasks',
                         headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None


# Functions used by base layer to unregister/reregister connectors 
# when no Kubernetes and/or Kafka relations are set.

def unregister_latest_connector():
    """Tries to unregister the connectors which were
    registered previously with register_connector().

    Returns True when succesfully unregistered or when 
    no previous connector has been set.
    """
    all_connectors = unitdata.kv().get('connectors', {})
    if all_connectors:
        for connector_name in all_connectors:
            response = unregister_connector(connector_name)
            if not response or (response.status_code != 204 and response.status_code != 404):
                log(response) if response else log("ERROR something went wrong with the connection")
                return False
    return True


def register_latest_connector():
    """Tries to register the connectors which were
    registered previously with register_connector().

    Returns True when succesfully registered or when 
    no previous connector has been set.
    """
    all_connectors = unitdata.kv().get('connectors', {})
    if all_connectors:
        for connector_name, connector_config in all_connectors.items():
            response = register_connector(connector_config, connector_name)
            if not response or not str(response.status_code).startswith('2'):
                log(response) if response else log("ERROR something went wrong with the connection")
                return False
    return True
