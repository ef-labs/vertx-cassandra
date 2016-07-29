# vertx-cassandra
Talk to Cassandra asynchronously from vert.x and run your callbacks on the vert.x event loop.

[![Build Status](http://img.shields.io/travis/ef-labs/vertx-cassandra.svg?maxAge=2592000&style=flat-square)](https://travis-ci.org/ef-labs/vertx-cassandra)
[![Maven Central](https://img.shields.io/maven-central/v/com.englishtown.vertx/vertx-cassandra.svg?maxAge=2592000&style=flat-square)](https://maven-badges.herokuapp.com/maven-central/com.englishtown.vertx/vertx-cassandra/)

## Getting Started

Add a dependency to vertx-cassandra:

```xml
<dependency>
    <groupId>com.englishtown.vertx</groupId>
    <artifactId>vertx-cassandra</artifactId>
    <version>3.2.0</version>
</dependency>
```

vert.x    | cassandra | vertx-cassandra
--------- | --------- | ---------------
3.3.0     |   3.7     | 3.4.0
3.2.0     |   3.0     | 3.2.0
3.1.0     |   2.1     | 3.1.0
3.0.0     |   2.1     | 3.0.0
2.x       |   2.1     | 2.1.0 (vertx-mod-cassandra)

## Configuration
The main configuration is via the normal config.json file for a Vert.x module. 

```json
{
    "cassandra": {
        "seeds": [<seeds>],
        
        "query": {
            "consistency_level": <string>,
            "serial_consistency_level": <string>,
            "fetch_size": <int>,
        },
        
        "policies": {
            "load_balancing": {
                "name": "<lb_policy_name>",
            },
            "reconnection": {
                "name": "<reconnect_policy_name>"
            }
        },
        
        "pooling": {
            "core_connections_per_host_local": <int>,
            "core_connections_per_host_remote": <int>,
            "max_connections_per_host_local": <int>,
            "max_connections_per_host_remote": <int>,
            "max_simultaneous_requests_local": <int>,
            "max_simultaneous_requests_remote": <int>
        },
        
        "socket": {
            "connect_timeout_millis": <int>,
            "read_timeout_millis": <int>,
            "keep_alive": <boolean>,
            "reuse_address": <boolean>,
            "receive_buffer_size": <int>,
            "send_buffer_size": <int>,
            "so_linger": <int>,
            "tcp_no_delay": <boolean>
        }
    }
}
```

* `seeds` - an array of string seed IP or host names.  At least one seed must be provided.
* `lb_policy_name` - (optional) the load balancing policy name.  The following values are accepted:
    * "DCAwareRoundRobinPolicy" - requires string field `local_dc` and optional numeric field `used_hosts_per_remote_dc`
    * Any FQCN such of a class that implements `LoadBalancingPolicy`
* `reconnect_policy_name` - (optional) the reconnect policy name.  The following values are accepted:
    * "constant"|"ConstantReconnectionPolicy" - creates a `ConstantReconnectionPolicy` policy.  Expects additional numeric       field `delay` in ms.
    * "exponential"|"ExponentialReconnectionPolicy" - creates an `ExponentialReconnectionPolicy` policy.  Expects               additional numeric fields `base_delay` and `max_delay` in ms.

Refer to the [Cassandra Java driver documentation](http://www.datastax.com/documentation/developer/java-driver/2.0/index.html) for a description of the remaining configuration options.


A sample config looks like:

```json
{
    "cassandra": {
        "seeds": ["10.0.0.1", "10.0.0.2"],
        
        "policies": {
            "load_balancing": {
                "name": "DCAwareRoundRobinPolicy",
                "local_dc": "LOCAL1",
                "used_hosts_per_remote_dc": 1
            },
            "reconnection": {
                "name": "exponential",
                "base_delay": 1000,
                "max_delay": 10000
            }
        },
    }
}
```

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
This module uses HK2 or Guice to provide an implementation of `CassandraSession` via injection. `CassandraSession` provides methods that allow statements to be executed, statements to be prepared and for the reading of metadata.

The execution and preparation methods have both synchronous and asynchronous variants. The asynchronous versions take a `FutureCallBack` class that is updated once the method has finished.

`FutureCallback` is part of [Guava](http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/util/concurrent/FutureCallback.html) and the JAR is included as part of the Datastax java driver used by this module.

The general best practice is to inject one `CassandraSession` per verticle.

### Promises Variant
There is a promises variant of `CassandraSession`, which is used by injecting `WhenCassandraSession` instead. This provides all of the same functionality, but instead of callbacks this class returns promises.

The promises used are from the Englishtown when.java package, which can be found on [Github](https://github.com/englishtown/when.java). 

If you intend to use this implementation, you must include the when.java dependency in your application as it is not provided by this module.
