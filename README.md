# Layer-kafka-connect-base

Kafka Connect is a framework to stream data into and out of Kafka. For more information see the [documentation](https://docs.confluent.io/current/connect/concepts.html#concepts).

## Operating a charm that uses this layer
This layer functions as a base to deploy and configure kafka connect workers in [distributed](https://docs.confluent.io/current/connect/userguide.html#distributed-mode) mode via Kubernetes.

The final charm **has to be colocated** with a Kafka unit where there Kafka libs should be available under `/usr/lib/kafka/bin`.

The layer adds multiple configuration options, these are common configuration parameters used by upper layers and needed in distributed worker configuration:
### Mandatory configs
- `workers` number of workers (Kubernetes pods) to be deployed. 
- `group-id` a unique string that identifies the Connect cluster group this worker belongs to. Defaults to the juju app name.
- `topics` a list of topics to use as input for this connector.
### Optional configs
- `worker-config` allows hard override on worker configs in the form of `property=value`. Multiple properties must be separated by a newline. Properties set via this config will override properties set by an upper layer / config.

## Developing a Charm with this layer
Include `layer:kafka-connect-base` in your `layer.yaml`. The base layer will create the needed connect topics and set the `kafka-connect-base.topic-created` state, then the layer will wait to create workers until an upper layer sets the flag `kafka-connect-base.install`. Helper methods are available in `charms.layer.kafka_connect_base.py`.

**Important**
To avoid conflicting REST calls, use the leadership layer so only the leader can execute Kafka Connect REST calls. If you want to set a status message on other (non-leader) units. The flag `kafka-connect-base.ready` is set when a relation is found for Kafka, kubernetes-deployer and if the topics config is set.

The workflow will be somewhat like this:
1. Wait until all needed relations are present. 
2. Wait until the `kafka-connect-base.topic-created` state is set.
3. Set the worker configuration with `set_worker_config(config)`
4. Set the flag `kafka-connect-base.install` to signal the base layer to deploy the workers to Kubernetes.
5. The base layer will set `kafka-connect.running` after deployment. You can now send connector configuration.
6. Use `register_connector(connector, connector_name)` to send the connector config to the workers.

By default the layer uses the docker image [sborny/kafka-connect-base](https://hub.docker.com/r/sborny/kafka-connect-base/). The docker README specifies which connectors are available for use.

### Helper methods
 Use the helper methods defined in `kakfa_connect_helpers.py` to setup / configure the workers.

***General functions:***
 - `set_worker_config(config)`  Set worker configs via a dict, a list of configuration is available [here](https://docs.confluent.io/current/connect/allconfigs.html#connect-allconfigs).
 - `get_worker_service()` Returns the ip:port of the Kubernetes service if available.
 - `set_base_image(image)` Set the docker image to deploy to Kubernetes
 - `get_configs_topic()` Name of the Kafka topic for the `config.storage.topic` configuration.
 - `get_offsets_topic()` Name of the Kafka topic for the `offsets.storage.topic` configuration.
 - `get_status_topic()` Name of the Kafka topic for the `status.storage.topic` configuration.

 ***REST api calls to the workers:***
 
 They all return a named tuple `Api_response` with the following format `Api_response(status_code, json)`.  If a `RequestException` is raised then `None` is returned. `connector` should be a dict with connector configs and `connector_name` is expected to be a string.
 - `register_connector(connector, connector_name)`
 - `unregister_connector(connector_name)`
 - `list_connectors()`
 - `connector_status(connector_name)`
 - `connector_restart(connector_name)`
 - `connector_pause(connector_name)`
 - `connector_resume(connector_name)`
 - `list_tasks(connector_name)`

## Example
```python
from charms.layer.kafka_connect_helpers import register_connector, set_worker_config

@when('mongodb.available',
      'kafka-connect-base.topic-created',
      'leadership.is_leader')
@when_not('kafka-connect-mongodb.configured')
def configure():	
    worker_configs = {
        'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
	    ...
    }
    set_worker_config(worker_configs)
    set_flag('kafka-connect-mongodb.configured')
    set_flag('kafka-connect-base.install')  # Tell the base layer a worker config is ready !


@when('kafka-connect.running',
      'mongodb.available',
      'leadership.is_leader')
@when_not('kafka-connect-mongodb.running')
def run():
    # Get MongoDB connection information
    connector_configs = {
        'connector.class': 'com.startapp.data.MongoSinkConnector',
        ...
    }
    response = register_connector(mongodb_connector_config, mongodb_connector_name)
    if response and (response.status_code == 200 or response.status_code == 201):
        status_set('active', 'ready')
        set_flag('kafka-connect-mongodb.running')  
```

## Caveats
- All config parameters except `worker-config` and `group-id` need to have at least a default configuration set even if you intend to set all configuration via an upper layer. Normally this should be a small concern since they all have a default value.
- The layer will create 3 Kafka topics for Kafka connect internal use. The number of partitions are hardcoded for best effort use and the replication factor is the number of Kafka brokers. The replication factor will **not** change after initial topic creation. The topics follow the following naming scheme:
```python
model = os.environ['JUJU_MODEL_NAME']
app = os.environ['JUJU_UNIT_NAME'].split('/')[0]
prefix = "{}.{}.".format(model, app)
 
offset.storage.topic = prefix +  '.connectoffsets' # 50 partitions
config.storage.topic = prefix  +  '.connectconfigs' # 1 partition
status.storage.topic = prefix +  '.connectstatus'   # 10 partitions
```

## Authors

This software was created in the [IBCN research group](https://www.ibcn.intec.ugent.be/) of [Ghent University](https://www.ugent.be/en) in Belgium. This software is used in [Tengu](https://tengu.io), a project that aims to make experimenting with data frameworks and tools as easy as possible.

 - Sander Borny <sander.borny@ugent.be>
