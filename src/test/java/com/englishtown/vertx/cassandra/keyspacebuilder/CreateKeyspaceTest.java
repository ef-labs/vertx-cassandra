package com.englishtown.vertx.cassandra.keyspacebuilder;

import com.datastax.driver.core.Statement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateKeyspaceTest {

    @Test
    public void testCreateKeyspace_SimpleStrategy() throws Exception {

        String name = "test_keyspace";

        Statement createKeyspace = KeyspaceBuilder.create(name)
                .ifNotExists()
                .simpleStrategy(3);

        String cql = createKeyspace.toString();
        assertEquals("CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }", cql);

    }

    @Test
    public void testCreateKeyspace_NetworkTopologyStrategy() throws Exception {

        String name = "test_keyspace";

        Statement createKeyspace = KeyspaceBuilder.create(name)
                .networkTopologyStrategy()
                .dc("dc1", 3)
                .dc("dc2", 2)
                .dc("dc3", 1);

        String cql = createKeyspace.toString();
        assertEquals("CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 2, 'dc3' : 1 }", cql);

    }

}