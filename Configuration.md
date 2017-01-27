# Configuration

Here is a list of the properties for GridDB.

|Property|Description|Required|Default Value|
|---|---|---|---|
|kairosdb.datastore.griddb.cluster_name|Name of GridDB cluster|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.user|User name to access GridDB cluster|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.password|User password to access GridDB cluster|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.notification_address|An IP address (IPV4 only) for receiving a notification used for autodetection of a GridDB master node|Required if using MULTICAST method to connect to GridDB cluster (cannot be specified with neither notification_member nor notification_provider properties at the same time)|239.0.0.1|
|kairosdb.datastore.griddb.notification_port|An IP port for receiving a notification used for autodetection of a GridDB master node|Required if notification_address is specified|31999|
|kairosdb.datastore.griddb.notification_member|"List of GridDB cluster nodes. In the form of: (Address1):(Port1),(Address2):(Port2),... "|Required if using FIXED LIST method to connect to GridDB cluster (cannot be specified with neither notification_address nor notification_provider properties at the same time)||
|kairosdb.datastore.griddb.notification_provider_url|URL of address provider used to connect to cluster which is configured with PROVIDER mode|Required if using PROVIDER method to connect to GridDB cluster (cannot be specified with neither notification_address nor notification_member properties at the same time)||
|kairosdb.datastore.griddb.consistency|Can be one of two consistency levels: IMMEDIATE or EVENTUAL|Optional|IMMEDIATE|
|kairosdb.datastore.griddb.container_cache_size|Size of cache to store GridDB ContainerInfos which are used to get Container without requesting to GridDB |Optional|0|
|kairosdb.datastore.griddb.data_affinity_pattern|GridDB affinity pattern for each container|Optional||
|kairosdb.datastore.griddb.failover_timeout|The minimum value of waiting time in seconds until a new destination is found in a failover|Optional|60|
|kairosdb.datastore.griddb.transaction_timeout|The minimum value of transaction timeout time in seconds|Optional|300|
|kairosdb.datastore.griddb.datapoint_ttl|Datapoint time to live in seconds (TTL). Datapoint is deleted automatically if its timestamp plus ttl is smaller than current time|Optional|0(forever)|
|kairosdb.datastore.griddb.max_data_cache_size|The maximum cache size created by GridDB module for processing datapoints|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.max_write_buffer_size|The maximum buffer size created by GridDB module for adding new datapoints|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.number_request_concurrency|The maximum number of connections at a time to GridDB cluster|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.write_delay|Time delay before data are written to GridDB from write buffer|Required if GridDB is the selected datastore.||
|kairosdb.datastore.griddb.write_buffer_datapoint|The maximum number of write buffer created for adding new datapoints|Required if GridDB is the selected datastore.||
