package com.englishtown.vertx.cassandra.keyspacebuilder;

import com.datastax.driver.core.Statement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DropKeyspaceTest {

    String keyspace = "test_keyspace";

    @Test
    public void testDropKeyspace() throws Exception {

        Statement statement = KeyspaceBuilder.drop(keyspace);
        String cql = statement.toString();

        assertEquals("DROP KEYSPACE test_keyspace", cql);

    }

    @Test
    public void testDropKeyspace_IfExists() throws Exception {

        Statement statement = KeyspaceBuilder.drop(keyspace)
                .ifExists();
        String cql = statement.toString();

        assertEquals("DROP KEYSPACE IF EXISTS test_keyspace", cql);

    }

}