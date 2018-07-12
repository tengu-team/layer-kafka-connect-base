import os
import yaml
import hashlib
import datetime
from subprocess import (
    CalledProcessError,
    Popen,
    PIPE,
    run,
)
from charms import leadership
from charms.layer import status
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
from charmhelpers.core.hookenv import (
    config,
    is_leader,
    log,
)
from charms.layer.kafka_connect_helpers import (
    register_latest_connector,
    unregister_latest_connector,
)


conf = config()


@when_not('endpoint.kubernetes.available')
def block_for_kubernetes():
    status.blocked('Waiting for Kubernetes deployer relation')


@when_not('kafka.ready')
def block_for_kafka():
    clear_flag('kafka-connect-base.topic-created')
    status.blocked('Waiting for Kafka relation')


@when_not('config.set.topics')
def block_for_topics():
    status.blocked('Waiting for topics configuration')


@when('endpoint.kubernetes.available',
      'kafka.ready',
      'config.set.topics')
def notify_upper_layer_ready():
    set_flag('kafka-connect-base.ready')


@when_not_all('endpoint.kubernetes.available',
              'kafka.ready',
              'config.set.topics')
def notify_upper_layer_not_ready():
    clear_flag('kafka-connect-base.ready')


@when_any('config.changed.workers',
          'config.changed.group-id',
          'config.changed.worker-config')
def config_changed():
    clear_flag('kafka-connect-base.configured')


@when('leadership.is_leader')
@when('kafka.ready',
      'kafka-connect-base.configured')
def check_kafka_changed():
    kafka = endpoint_from_flag('kafka.ready')
    if data_changed('kafka_info', kafka.kafkas()):
        clear_flag('kafka-connect-base.configured')


@when('kafka-connect-base.install', 
      'leadership.is_leader')
@when_not('kafka-connect-base.installed')
def install_kafka_connect_base():
    if not os.path.exists('/etc/kafka-connect'):
        os.makedirs('/etc/kafka-connect')
    if not unitdata.kv().get('docker-image', None):
        unitdata.kv().set('docker-image', 'sborny/kafka-connect-base')
    set_flag('kafka-connect-base.installed')


@when('kafka.ready',
      'leadership.is_leader')
@when_not('kafka-connect-base.topic-created')
def create_topics():
    if not os.path.exists('/usr/lib/kafka/bin'):
        status.blocked('Could not find Kafka library, make sure the Kafka Connect charm is colocated with a Kafka unit.')
        return
    kafka = endpoint_from_flag('kafka.ready')
    zookeepers = []
    for zookeeper in kafka.zookeepers():
        zookeepers.append(zookeeper['host'] + ":" + zookeeper['port'])
    if not zookeepers:
        return
    # Set replication factor as number of Kafka brokers
    # Use the zookeeper-shell because Juju sets the Kafka
    # broker info one hook at a time and therefore we do not 
    # know beforehand howmany brokers there are    
    p = Popen(['/usr/lib/kafka/bin/zookeeper-shell.sh',
                   zookeepers[0]],
               stdin=PIPE, 
               stdout=PIPE)
    output = p.communicate(b'ls /brokers/ids')[0]
    # The broker info is in the last line between [id1,id2,id3]
    replication_factor = output.decode('utf-8') \
                               .strip() \
                               .split('\n')[-1] \
                               .count(',') \
                               + 1
    topics_suffixes = ['connectconfigs', 'connectoffsets', 'connectstatus']
    partitions = [1, 50, 10] # Best effort partition numbers
    model = os.environ['JUJU_MODEL_NAME']
    app = os.environ['JUJU_UNIT_NAME'].split('/')[0]
    prefix = "{}.{}.".format(model, app)
    for (suffix, partitions) in zip(topics_suffixes, partitions):
        topic = prefix + suffix
        unitdata.kv().set(suffix, topic)
        if topic_exists(topic, zookeepers):
            continue
        try:
            output = run(['/usr/lib/kafka/bin/kafka-topics.sh',
                          '--zookeeper',
                          ",".join(zookeepers),
                          "--create",
                          "--topic",
                          topic,
                          "--partitions",
                          str(partitions),
                          "--replication-factor",
                          str(replication_factor)], stdout=PIPE)
            output.check_returncode()
        except CalledProcessError as e:
            log(e)
    set_flag('kafka-connect-base.topic-created')


@when('config.set.topics',
      'config.set.workers',
      'endpoint.kubernetes.available',
      'kafka.ready',
      'kafka-connect-base.installed',
      'kafka-connect-base.topic-created',
      'leadership.is_leader')
@when_not('kafka-connect-base.configured')
def configure_kafka_connect_base():
    kafka = endpoint_from_flag('kafka.ready')
    kubernetes = endpoint_from_flag('endpoint.kubernetes.available')

    kafka_brokers = []
    for kafka_unit in kafka.kafkas():
        kafka_brokers.append(kafka_unit['host'] + ':' + kafka_unit['port'])

    worker_config = generate_worker_config()
    worker_config['bootstrap.servers'] = ','.join(kafka_brokers)
    port = worker_config['rest.port'] if 'rest.port' in worker_config else 8083

    uuid = kubernetes.get_uuid()

    resource_context = {
        'configmap_name': 'cfgmap-{}'.format(uuid),
        'label': uuid,
        'properties': worker_config,
        'service_name': 'svc-{}'.format(uuid),
        'port': port,
        'deployment_name': 'depl-{}'.format(uuid),
        'replicas': conf.get('workers', 1),
        'container_name': uuid,
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

        
        resources = []
        with open('/etc/kafka-connect/resources.yaml', 'r') as f:
            docs = yaml.load_all(f)
            for doc in docs:
                resources.append(doc)
        kubernetes.send_create_request(resources)

    status.waiting('Waiting for k8s deployment (will happen in next hook)')
    set_flag('kafka-connect-base.configured')


@when('endpoint.kubernetes.new-status',
      'leadership.is_leader')
@when_not('kafka-connect-base.configured')
def remove_kubernetes_status_update():
    clear_flag('endpoint.kubernetes.new-status')


@when('endpoint.kubernetes.new-status',
      'kafka-connect-base.configured',
      'leadership.is_leader')
def kubernetes_status_update():
    kubernetes = endpoint_from_flag('endpoint.kubernetes.new-status')    
    k8s_status = kubernetes.get_status()
    if not k8s_status or not k8s_status['status']:
        return
    
    nodeport = None
    deployment_running = False
    # Check if service and deployment has been created on k8s
    # If the service is created, set the connection string
    # else clear it.
    for resource in k8s_status['status']:
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
        status.active('K8s deployment running')
        clear_flag('endpoint.kubernetes.new-status')
        set_flag('kafka-connect.running')
    else:
        unitdata.kv().set('kafka-connect-service', '')
        clear_flag('kafka-connect.running')


@when('website.available',
      'kafka-connect.running',
      'leadership.is_leader')
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


@when('leadership.is_leader')
@when_any('kafka-connect-base.configured',
          'kafka-connect.running')
@when_not_all('kafka.ready',
              'endpoint.kubernetes.available')
def reset_base_flags():
    data_changed('resource-context', {})
    clear_flag('kafka-connect-base.configured')
    clear_flag('kafka-connect.running')
    status.maintenance('Unregistering connector')
    if unregister_latest_connector():
        set_flag('kafka-connect-base.unregistered')
    else:
        status.blocked('Could not unregister connectors')


@when('kafka-connect-base.unregistered',
      'kafka-connect.running',
      'leadership.is_leader')
def reregister_connector():
    status.maintenance('Reregistering connector')
    if not register_latest_connector():
        status.blocked('Could not reregister previous connectors, trying next hook..')
    else:
        status.active('ready')
        clear_flag('kafka-connect-base.unregistered')


def topic_exists(topic_name, zookeepers):
    try:
        cmd = [
            '/usr/lib/kafka/bin/kafka-topics.sh',
            '--zookeeper',
            ','.join(zookeepers),
            '--list'
        ]
        output = run(cmd, stdout=PIPE)
        output.check_returncode()

        topics = output.stdout.decode('utf-8').rstrip().split('\n')
        return True if topic_name in topics else False
    except CalledProcessError as e:
        log(e)
    return None