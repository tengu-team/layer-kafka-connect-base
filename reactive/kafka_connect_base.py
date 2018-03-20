import os
import yaml
import hashlib
import datetime
from charms.reactive import (
    when,
    when_any,
    when_not,
    set_flag,
    clear_flag,
    when_not_all,
)
from charms.reactive.helpers import data_changed
from charms.reactive.relations import endpoint_from_flag
from charmhelpers.core import unitdata, templating
from charmhelpers.core.hookenv import status_set, config
from charms.layer.kafka_connect_helpers import (
    register_latest_connector,
    unregister_latest_connector,
)


conf = config()


@when_not('endpoint.kubernetes.available')
def block_for_kubernetes():
    status_set('blocked', 'Waiting for Kubernetes deployer relation')


@when_not('kafka.ready')
def block_for_kafka():
    status_set('blocked', 'Waiting for Kafka relation')


@when_not('config.set.topics')
def block_for_topics():
    status_set('blocked', 'Waiting for topics configuration')


@when_not('endpoint.kafka-topic.available')
def block_for_kafka_topics():
    status_set('blocked', 'Waiting on all 3 topic relations (config, offsets and status)')


@when_any('config.changed.workers',
          'config.changed.group-id',
          'config.changed.worker-config')
def config_changed():
    clear_flag('kafka-connect-base.configured')


@when('kafka.ready',
      'kafka-connect-base.configured')
def check_kafka_changed():
    kafka = endpoint_from_flag('kafka.ready')
    if data_changed('kafka_info', kafka.kafkas()):
        clear_flag('kafka-connect-base.configured')


@when('kafka-connect-base.install')
@when_not('kafka-connect-base.installed')
def install_kafka_connect_base():
    if not os.path.exists('/etc/kafka-connect'):
        os.makedirs('/etc/kafka-connect')
    if not unitdata.kv().get('docker-image', None):
        unitdata.kv().set('docker-image', 'sborny/kafka-connect-base')
    set_flag('kafka-connect-base.installed')


@when('endpoint.kafka-topic.new-topic-info')
@when_not('kafka-connect-base.topic-created')
def check_kafka_topics_created():
    kafka_topics = endpoint_from_flag('endpoint.kafka-topic.new-topic-info')
    topics_info = kafka_topics.get_topics()
    if len(topics_info) == 3:
        set_flag('kafka-connect-base.topic-created')
    else:
        status_set('blocked', 'Waiting on all 3 topic relations (config, offsets and status)')
    clear_flag('endpoint.kafka-topics.new-topic-info')


@when('config.set.topics',
      'config.set.workers',
      'endpoint.kubernetes.available',
      'kafka.ready',
      'kafka-connect-base.installed',
      'kafka-connect-base.topic-created')
@when_not('kafka-connect-base.configured')
def configure_kafka_connect_base():
    kafka = endpoint_from_flag('kafka.ready')
    kafka_brokers = []
    for kafka_unit in kafka.kafkas():
        kafka_brokers.append(kafka_unit['host'] + ':' + kafka_unit['port'])

    juju_app_name = os.environ['JUJU_UNIT_NAME'].split('/')[0]

    worker_config = generate_worker_config()
    worker_config['bootstrap.servers'] = ','.join(kafka_brokers)
    port = worker_config['rest.port'] if 'rest.port' in worker_config else 8083

    resource_context = {
        'configmap_name': juju_app_name + '-cfgmap',
        'label': 'kafka-connect-' + juju_app_name,
        'properties': worker_config,
        'service_name': juju_app_name + '-service',
        'port': port,
        'deployment_name': juju_app_name + '-deployment',
        'replicas': conf.get('workers', 1),
        'container_name': juju_app_name,
        'image': unitdata.kv().get('docker-image'),
        'containerport': port,
    }

    if data_changed('resource-context', resource_context):
        # Trigger a rolling update by setting a new annotation in the deployment
        resource_context['configmap_annotation'] = hashlib.sha1(datetime.datetime.now()
                                                                .isoformat()
                                                                .encode('utf-8')).hexdigest()
        templating.render(source="resources.j2",
                          target="/etc/kafka-connect/resources.yaml",
                          context=resource_context)

        kubernetes = endpoint_from_flag('endpoint.kubernetes.available')
        resources = []
        with open('/etc/kafka-connect/resources.yaml', 'r') as f:
            docs = yaml.load_all(f)
            for doc in docs:
                resources.append(doc)
        kubernetes.send_create_request(resources)

    status_set('waiting', 'Waiting for k8s deployment (will happen in next hook)')
    set_flag('kafka-connect-base.configured')


@when('endpoint.kubernetes.new-status')
@when_not('kafka-connect-base.configured')
def remove_kubernetes_status_update():
    clear_flag('endpoint.kubernetes.new-status')


@when('endpoint.kubernetes.new-status',
      'kafka-connect-base.configured')
def kubernetes_status_update():
    kubernetes = endpoint_from_flag('endpoint.kubernetes.new-status')    
    status = kubernetes.get_status()
    if not status or not status['status']:
        return
    uuid = kubernetes.get_uuid()
    nodeport = None
    deployment_running = False
    # Check if service and deployment has been created on k8s
    # If the service is created, set the connection string
    # else clear it.
    for resources in status['status']:
        if uuid == resources:
            for resource in status['status'][resources]:
                if resource['kind'] == "Service":
                    nodeport = resource['spec']['ports'][0]['nodePort']
                elif resource['kind'] == "Deployment":
                    if 'availableReplicas' in resource['status'] and \
                        resource['status']['availableReplicas'] == \
                        resource['status']['readyReplicas']:
                        deployment_running = True
    kubernetes_workers = kubernetes.get_worker_ips()
    if nodeport and kubernetes_workers and deployment_running:
        unitdata.kv().set('kafka-connect-service',
                          kubernetes_workers[0] + ':' + str(nodeport))
        status_set('active', 'K8s deployment running')
        clear_flag('endpoint.kubernetes.new-status')  # TODO try
        set_flag('kafka-connect.running')
    else:
        unitdata.kv().set('kafka-connect-service', '')
        clear_flag('kafka-connect.running')


@when('website.available',
      'kafka-connect.running')
def website_available():
    website = endpoint_from_flag('website.available')
    service = unitdata.kv().get('kafka-connect-service')
    website.configure(hostname=service.split(':')[0], port=service.split(':')[1])


def generate_worker_config():
    # Get worker config set from upper layer and
    # overwrite values set via config worker-config
    properties = unitdata.kv().get('worker.properties', {})
    if 'group.id' not in properties:
        if conf.get('group-id') == '':
            properties['group.id'] = os.environ['JUJU_UNIT_NAME'].replace('/', '-')
        else:
            properties['group.id'] = conf.get('group-id')
    if conf.get('worker-config'):
        worker_config = conf.get('worker-config').rstrip('\n')
        worker_config = worker_config.split('\n')
        override = {}
        for config in worker_config:
            key, value = config.split('=')
            override[key] = value.rstrip()
        properties.update(override)
    return properties


@when_any('kafka-connect-base.configured',
          'kafka-connect.running')
@when_not_all('kafka.ready',
              'endpoint.kubernetes.available')
def reset_base_flags():
    data_changed('resource-context', {})
    clear_flag('kafka-connect-base.configured')
    clear_flag('kafka-connect.running')
    status_set('maintenance', 'Unregistering connector')
    if unregister_latest_connector():
        set_flag('kafka-connect-base.unregistered')
    else:
        status_set('blocked', 'Could not unregister connectors')


@when('kafka-connect-base.unregistered',
      'kafka-connect.running')
def reregister_connector():
    status_set('maintenance', 'Reregistering connector')
    if not register_latest_connector():
        status_set('blocked', 'Could not reregister previous connectors, trying next hook..')
    else:
        status_set('active', 'ready')
        clear_flag('kafka-connect-base.unregistered')
