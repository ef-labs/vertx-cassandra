# vertx-cassandra-mapping

Provides vert.x wrappers for com.datastax.cassandra:cassandra-driver-mapping objects to run on the vert.x context.

## Getting Started

Add a dependency to

```xml
<dependency>
    <groupId>com.englishtown.vertx</groupId>
    <artifactId>vertx-cassandra-mapping</artifactId>
    <version>3.0.0-SNAPSHOT</version>
</dependency>
```

Inject or create an instance of `com.englishtown.vertx.cassandra.mapping.VertxMappingManager` and create a POJO with the datastax mapping annotations (`@Table`, `@Column`, `@PartitionKey`, etc.).

## Examples

#### Save

```java
// Get a mapper from the vert.x mapping manager
VertxMappingManager mappingManager = getMappingManager();
VertxMapper<MyEntity> mapper = mappingManager.mapper(MyEntity.class);

MyEntity entity = getEntity();

mapper.saveAsync(entity, new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void aVoid) {
            // Entity saved successfully
        }

        @Override
        public void onFailure(Throwable t) {
            // Entity save failed
        }
    });
```

#### Get

```java
// Get a mapper from the vert.x mapping manager
VertxMappingManager mappingManager = getMappingManager();
VertxMapper<MyEntity> mapper = mappingManager.mapper(MyEntity.class);

String entityId = "123";

mapper.getAsync(new FutureCallback<MyEntity>() {
        @Override
        public void onSuccess(MyEntity entity) {
            // Entity loaded from cassandra (if it existed)
        }

        @Override
        public void onFailure(Throwable t) {
            // Entity failed to load
        }
    }, entityId);
```

See the [integration test](src/test/java/com/englishtown/vertx/cassandra/mapping/integration/MappingIntegrationTest.java) for more details.


## Constructing VertxMappingManager

#### Dependency Injection

Guice and HK2 Binders have been provided for injecting a singleton instance of `VertxMappingManager`

* `com.englishtown.vertx.cassandra.mapping.guice.GuiceMappingBinder`
* `com.englishtown.vertx.cassandra.mapping.hk2.HK2MappingBinder`
 
#### Manual Instantiation

The `VertxMappingManager` can be instantiated by passing your instance of `CassandraSession`:

```java
CassandraSession session = getSession();
VertxMappingManager mappingManager = new DefaultVertxMappingManager(session);
```

## Promises

A when.java version has also been provided `com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMappingManager` to work with promises.

See the [when integration test](src/test/java/com/englishtown/vertx/cassandra/mapping/integration/WhenMappingIntegrationTest.java) for more details.