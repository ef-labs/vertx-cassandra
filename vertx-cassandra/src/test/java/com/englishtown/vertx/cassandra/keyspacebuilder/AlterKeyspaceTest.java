package com.englishtown.vertx.cassandra.keyspacebuilder;

import com.datastax.driver.core.Statement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlterKeyspaceTest {

    @Test
    public void testAlterKeyspace_SimpleStrategy() throws Exception {

        String name = "test_keyspace";

        Statement alterKeyspace = KeyspaceBuilder.alter(name)
                .simpleStrategy(3);

        String cql = alterKeyspace.toString();
        assertEquals("ALTER KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }", cql);

    }

    @Test
    public void testCreateKeyspace_NetworkTopologyStrategy() throws Exception {

        String name = "test_keyspace";

        Statement createKeyspace = KeyspaceBuilder.alter(name)
                .networkTopologyStrategy()
                .dc("dc1", 3)
                .dc("dc2", 2)
                .dc("dc3", 1);

        String cql = createKeyspace.toString();
        assertEquals("ALTER KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 2, 'dc3' : 1 }", cql);

    }

}