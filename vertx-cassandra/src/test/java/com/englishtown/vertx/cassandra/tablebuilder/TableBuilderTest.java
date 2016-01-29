package com.englishtown.vertx.cassandra.tablebuilder;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TableBuilderTest {

    private TestTableMetadata existing;
    @Mock
    private ColumnMetadata col1;
    @Mock
    private ColumnMetadata col2;

    @Before
    public void setUp() throws Exception {

        existing = new TestTableMetadata();
        existing.getColumnMap().put("col1", col1);
        existing.getColumnMap().put("col2", col2);

        when(col1.getType()).thenReturn(DataType.text());
        when(col2.getType()).thenReturn(DataType.cint());

    }

    @Test
    public void testAlter() throws Exception {

        CreateTable desired = TableBuilder.create("test_keyspace", "test_table")
                .column("col1", "text")
                .column("col2", "bigint")
                .column("col3", "int")
                .column("col4", "text")
                .primaryKey("col1");

        List<AlterTable> statements = TableBuilder.alter(existing, desired);

        assertEquals(3, statements.size());
        assertEquals("ALTER TABLE test_keyspace.test_table ALTER col2 TYPE bigint", statements.get(0).toString());
        assertEquals("ALTER TABLE test_keyspace.test_table ADD col3 int", statements.get(1).toString());
        assertEquals("ALTER TABLE test_keyspace.test_table ADD col4 text", statements.get(2).toString());

    }

    private static class TestTableMetadata extends AbstractTableMetadata {

        public TestTableMetadata() {
            super(null, null, null, null, null, new HashMap<>(), null, null, null);
        }

        @Override
        protected String asCQLQuery(boolean formatted) {
            throw new UnsupportedOperationException();
        }

        public Map<String, ColumnMetadata> getColumnMap() {
            return columns;
        }
    }
}