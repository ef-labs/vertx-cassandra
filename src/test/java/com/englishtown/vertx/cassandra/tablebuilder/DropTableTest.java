package com.englishtown.vertx.cassandra.tablebuilder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DropTableTest {

    @Test
    public void testDropTable() throws Exception {

        DropTable table = TableBuilder.drop("test_keyspace", "test_table");

        String cql = table.getQueryString();
        assertEquals("DROP TABLE test_keyspace.test_table", cql);

    }

    @Test
    public void testIfExists() throws Exception {

        DropTable table = TableBuilder.drop("test_keyspace", "test_table").ifExists();

        String cql = table.getQueryString();
        assertEquals("DROP TABLE IF EXISTS test_keyspace.test_table", cql);

    }

}