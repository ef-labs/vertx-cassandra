package com.englishtown.vertx.cassandra.tablebuilder;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateTableTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testCreateTable() throws Exception {

        CreateTable table = TableBuilder.create("test_keyspace", "test_table")
                .ifNotExists()
                .column("col1", "text")
                .column("col2", "int")
                .staticColumn("col3", "blob")
                .primaryKeys("col1", "col2");

        String cql = table.getQueryString();
        assertEquals("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (col1 text, col2 int, col3 blob STATIC, PRIMARY KEY(col1, col2))", cql);

    }

    @Test
    public void testCreateTable_Composite_Partition_Key() throws Exception {

        CreateTable table = TableBuilder.create("test_keyspace", "test_table")
                .column("col1", "text")
                .column("col2", "int")
                .column("col3", "blob")
                .column("col4", "uuid")
                .primaryKey("col1", PrimaryKeyType.PARTITIONING)
                .primaryKey("col2", PrimaryKeyType.PARTITIONING)
                .primaryKey("col3", PrimaryKeyType.CLUSTERING)
                .primaryKey("col4");

        String cql = table.getQueryString();
        assertEquals("CREATE TABLE test_keyspace.test_table (col1 text, col2 int, col3 blob, col4 uuid, PRIMARY KEY((col1, col2), col3, col4))", cql);

    }

    @Test
    public void testColumns() throws Exception {

        CreateTable table = TableBuilder.create("test_keyspace", "test_table")
                .columns(new String[]{"col1", "col2", "col3"}, new String[]{"text", "int", "blob"})
                .column("col4", "text")
                .column("col3", "text")
                .column("col1", "int")
                .primaryKey("col1");

        String cql = table.getQueryString();
        assertEquals("CREATE TABLE test_keyspace.test_table (col1 int, col2 int, col3 text, col4 text, PRIMARY KEY(col1))", cql);

    }

}