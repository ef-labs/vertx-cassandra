package com.englishtown.vertx.cassandra.tablebuilder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlterTableTest {

    @Test
    public void testAlter() throws Exception {
        AlterTable alter = TableBuilder.alter("test_keyspace", "test_table")
                .alterColumn("col1", "text");

        String cql = alter.getQueryString();
        assertEquals("ALTER TABLE test_keyspace.test_table ALTER col1 TYPE text", cql);
    }

    @Test
    public void testAdd() throws Exception {
        AlterTable alter = TableBuilder.alter("test_keyspace", "test_table")
                .addColumn("col1", "text");

        String cql = alter.getQueryString();
        assertEquals("ALTER TABLE test_keyspace.test_table ADD col1 text", cql);
    }

    @Test
    public void testDrop() throws Exception {
        AlterTable alter = TableBuilder.alter("test_keyspace", "test_table")
                .dropColumn("col1");

        String cql = alter.getQueryString();
        assertEquals("ALTER TABLE test_keyspace.test_table DROP col1", cql);
    }

    @Test
    public void testRename() throws Exception {
        AlterTable alter = TableBuilder.alter("test_keyspace", "test_table")
                .renameColumn("col1", "col2");

        String cql = alter.getQueryString();
        assertEquals("ALTER TABLE test_keyspace.test_table RENAME col1 TO col2", cql);
    }
}