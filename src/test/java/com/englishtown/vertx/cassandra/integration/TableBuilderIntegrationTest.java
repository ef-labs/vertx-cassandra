package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.englishtown.vertx.cassandra.tablebuilder.AlterTable;
import com.englishtown.vertx.cassandra.tablebuilder.CreateTable;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import org.junit.Test;
import org.vertx.testtools.VertxAssert;

import java.util.List;

/**
 * Integration tests for {@link com.englishtown.vertx.cassandra.tablebuilder.TableBuilder}
 */
public class TableBuilderIntegrationTest extends IntegrationTestBase {

    @Test
    public void testTableBuilder() throws Exception {

        VertxAssert.assertNotNull(session.getMetadata().getKeyspace(keyspace));

        String table = "test_table";
        Statement statement;

        VertxAssert.assertNull(session.getMetadata().getKeyspace(keyspace).getTable(table));

        // Create the table
        statement = TableBuilder.create(keyspace, table)
                .column("col1", "text")
                .column("col2", "text")
                .primaryKey("col1");

        session.execute(statement);

        VertxAssert.assertNotNull(session.getMetadata().getKeyspace(keyspace).getTable(table));

        try {
            session.execute(statement);
            VertxAssert.fail("Table already exists, create should have throw AlreadyExistsException");
        } catch (QueryValidationException e) {
            // Expected
        }

        // Should not fail with the "IF NOT EXISTS" statement
        statement = TableBuilder.create(keyspace, table)
                .ifNotExists()
                .column("col1", "text")
                .column("col2", "varchar")
                .primaryKey("col1");

        session.execute(statement);

        CreateTable createTable = TableBuilder.create(keyspace, table)
                .column("col1", "text")
                .column("col2", "varchar")
                .column("col3", "int")
                .column("col4", "boolean")
                .column("col5", "uuid")
                .primaryKey("col1");

        TableMetadata tableMetadata = session.getMetadata().getKeyspace(keyspace).getTable(table);
        List<AlterTable> alterStatements = TableBuilder.alter(tableMetadata, createTable);

        for (AlterTable alterTable : alterStatements) {
            session.execute(alterTable);
        }

        tableMetadata = session.getMetadata().getKeyspace(keyspace).getTable(table);
        VertxAssert.assertEquals(5, tableMetadata.getColumns().size());

        statement = TableBuilder.drop(keyspace, table);
        session.execute(statement);

        statement = TableBuilder.drop(keyspace, table)
                .ifExists();
        session.execute(statement);

        try {
            statement = TableBuilder.drop(keyspace, table);
            session.execute(statement);
            VertxAssert.fail("Table should not exist, drop should have throw InvalidQueryException");
        } catch (QueryValidationException e) {
            // Expected
        }

        VertxAssert.testComplete();

    }

}
