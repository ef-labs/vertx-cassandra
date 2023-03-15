# vertx-cassandra

Talk to Cassandra asynchronously from vert.x and run your callbacks on the vert.x event loop.

[![Build Status](http://img.shields.io/travis/ef-labs/vertx-cassandra.svg?maxAge=2592000&style=flat-square)](https://travis-ci.org/ef-labs/vertx-cassandra)
[![Maven Central](https://img.shields.io/maven-central/v/com.englishtown.vertx/vertx-cassandra.svg?maxAge=2592000&style=flat-square)](https://maven-badges.herokuapp.com/maven-central/com.englishtown.vertx/vertx-cassandra/)

## Getting Started

Add a dependency to `vertx-cassandra`:

```xml
<dependency>
    <groupId>com.englishtown.vertx</groupId>
    <artifactId>vertx-cassandra</artifactId>
    <version>4.0.0</version>
</dependency>
```

| vertx-cassandra | vert.x | cassandra |
|-----------------|--------|-----------|
| 4.0.0           | 3.5.0  | 4.x       |
| 3.6.0           | 3.5.0  | 3.11      |
| 3.4.0           | 3.3.0  | 3.7       |
| 3.2.0           | 3.2.0  | 3.0       |
| 3.1.0           | 3.1.0  | 2.1       |
| 3.0.0           | 3.0.0  | 2.1       |

## Configuration
The main configuration is via the normal config.json file for a Vert.x module. 

```json
{
    "cassandra": {
      "seeds": [<seeds>],
      "local_dc": <local_dc>
    }
}
```

* `seeds` - an array of inet socket address strings `ip:port`.
* `local_dc` - the local datacenter to connect to

Refer to the [Cassandra Java driver documentation](http://www.datastax.com/documentation/developer/java-driver/2.0/index.html) for a description of the remaining configuration options.


A sample config looks like:

```json
{
    "cassandra": {
        "seeds": ["10.0.0.1", "10.0.0.2"]
    }
}
```

### Overriding with Environment Variables

It is possible to override the seeds and policies section of the configuration using environment variables. The two environment variables are:

    CASSANDRA_SEEDS="{ip:port}|{ip}|{ip}..."
    CASSANDRA_LOCAL_DC=
    
So an example would be:

    CASSANDRA_SEEDS="10.0.0.3:9043|10.0.0.4"
    CASSANDRA_LOCAL_DC="datacenter1"

So the examples given here are akin to having this in the config file:

    "seeds": ["10.0.0.3", "10.0.0.4"],

    
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
