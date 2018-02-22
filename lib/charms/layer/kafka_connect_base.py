import requests
import collections
from charms.reactive import clear_flag
from charmhelpers.core import unitdata


def set_worker_config(config):
    '''https://docs.confluent.io/current/connect/allconfigs.html#connect-allconfigs
    '''
    unitdata.kv().set('worker.properties', config)
    clear_flag('kafka-connect-base.configured') # Is dit nodig?


def set_base_image(image):
    unitdata.kv().set('docker-image', image)
    clear_flag('kafka-connect-base.configured')


def get_worker_service():
    return unitdata.kv().get('kafka-connect-service', '')


# https://docs.confluent.io/current/connect/restapi.html
# TODO document response
Api_response = collections.namedtuple('Api_response', ['status_code', 'json'])

def register_connector(connector):
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        r = requests.post("http://" + get_worker_service() + "/connectors",
                        json=connector,
                        headers=headers)
        return Api_response(r.status_code, r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        return None


def register_update_connector(connector, connector_name):
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
