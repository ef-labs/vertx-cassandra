# vertx-mod-cassandra
Non-runnable module that provides functionality for talking to Cassandra.

## Configuration
The main configuration is via the normal config.json file for a Vert.x module. A sample config looks like:

    "cassandra": {
        "seeds": ["10.0.0.1", "10.0.0.2"],
        
        "policies": {
            "load_balancing": {
                "name": "DCAwareRoundRobinPolicy",
                "local_dc": "LOCAL1",
                "used_hosts_per_remote_dc": 1
            }
        },
        
        "pooling": {
            "core_connections_per_host_local": 1,
            "core_connections_per_host_remote": 1,
            "max_connections_per_host_local": 10,
            "max_connections_per_host_remote": 10,
            "min_simultaneous_requests_local": 1,
            "min_simultaneous_requests_remote": 1,
            "max_simultaneous_requests_local": 3,
            "max_simultaneous_requests_remote": 3
        },
        
        "socket": {
            "connect_timeout_millis": 30000,
            "read_timeout_millis": 12000,
            "keep_alive": true,
            "reuse_address": false,
            "receive_buffer_size": 30000,
            "send_buffer_size": 30000,
            "so_linger": 100,
            "tcp_no_delay": true
        }
    }

Please refer to the [Cassandra Java driver documentation](http://www.datastax.com/documentation/developer/java-driver/2.0/java-driver/whatsNew2.html) for a description of what these configuration options do.

### Overriding with Environment Variables
It is possible to override the seeds and policies section of the configuration using environment variables. The two environment variables are:

    CASSANDRA_SEEDS="{ip}|{ip}|{ip}..."
    CASSANDRA_LOCAL_DC="{Local DC Name}"
    
So an example would be:

    CASSANDRA_SEEDS="10.0.0.3|10.0.0.4"
    CASSANDRA_LOCAL_DC="REMOTE1"
    
The latter, the `CASSANDRA_LOCAL_DC`, if used will override the entire load balancing section of the configuration, setting it to `DCAwareRoundRobinPolicy`, with a `used_hosts` value of 0 and the `local_dc` set to whatever is placed in the environment variable. 

So the examples given here are akin to having this in the config file:

    "seeds": ["10.0.0.3", "10.0.0.4"],
    "policies": {
        "load_balancing": {
            "name": "DCAwareRoundRobinPolicy",
            "local_dc": "REMOTE1",
            "used_hosts_per_remote_dc": 0
        }
    }
    
### Defaults
If there is no configuration, neither JSON nor environment variables, the module will default to looking for cassandra at 127.0.0.1 with the Cassandra driver defaults for everything.

## How to Use
This module uses HK2 to provide an implementation of `CassandraSession` via injection. `CassandraSession` provides methods that allow statements to be executed, statements to be prepared and for the reading of metadata.

The execution and preparation methods have both synchronous and asynchronous variants. The aysnchronous versions take a `FutureCallBack` class that is updated once the method has finished.

`FutureCallback` is part of [Guava](http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/util/concurrent/FutureCallback.html) and the JAR is included as part of the Datastax java driver used by this module.

### Promises Variant
There is a promises variant of `CassandraSession`, which is used by injecting `WhenCassandraSession` instead. This provides all of the same functionality, but instead of callbacks this class returns promises.

The promises used are from the Englishtown when.java package, which can be found on [Github](https://github.com/englishtown/when.java). 

If you intend to use this implementation, you must include the when.java dependency in your application as it is not provided by this module.