package com.englishtown.vertx.cassandra.tablebuilder;

import com.datastax.driver.core.*;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableBuilderTest {

    @Test
    public void testAlter() throws Exception {

        CreateTable desired = TableBuilder.create("test_keyspace", "test_table")
                .column("col1", "text")
                .column("col2", "bigint")
                .column("col3", "int")
                .column("col4", "text")
                .primaryKey("col1");

        TableMetadata existing = mock(TableMetadata.class);

        ColumnMetadata col1 = mock(ColumnMetadata.class);
        when(existing.getColumn(eq("col1"))).thenReturn(col1);
        when(col1.getType()).thenReturn(DataType.text());

        ColumnMetadata col2 = mock(ColumnMetadata.class);
        when(existing.getColumn(eq("col2"))).thenReturn(col2);
        when(col2.getType()).thenReturn(DataType.cint());

        BatchStatement batch = TableBuilder.alter(existing, desired);
        Collection<Statement> statements = batch.getStatements();

        assertEquals(3, statements.size());
        Statement[] arr = statements.toArray(new Statement[3]);
        assertEquals("ALTER TABLE test_keyspace.test_table ALTER col2 TYPE bigint", arr[0].toString());
        assertEquals("ALTER TABLE test_keyspace.test_table ADD col3 int", arr[1].toString());
        assertEquals("ALTER TABLE test_keyspace.test_table ADD col4 text", arr[2].toString());

    }
}