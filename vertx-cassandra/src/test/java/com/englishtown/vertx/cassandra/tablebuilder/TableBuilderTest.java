package com.englishtown.vertx.cassandra.tablebuilder;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TableBuilderTest {

    @Mock
    private TableMetadata existing;
    @Mock
    private ColumnMetadata col1;
    @Mock
    private ColumnMetadata col2;

    private Map<String, ColumnMetadata> columns = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        if (!setUpColumns()) {
            throw new IllegalStateException("Could not set up columns");
        }

        columns.put("col1", col1);
        when(col1.getType()).thenReturn(DataType.text());

        columns.put("col2", col2);
        when(col2.getType()).thenReturn(DataType.cint());

    }

    private boolean setUpColumns() throws Exception {

        // Need to set columns as of DSE 3.0....sigh
        Class<?> clazz = existing.getClass();

        while (clazz != Object.class) {
            Field[] fields = clazz.getDeclaredFields();

            for (Field field : fields) {
                if (field.getName().equals("columns")) {
                    field.setAccessible(true);
                    field.set(existing, columns);
                    return true;
                }
            }
            clazz = clazz.getSuperclass();
        }

        return false;
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
}