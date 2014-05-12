package com.englishtown.vertx.cassandra.tablebuilder;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;

/**
 * Static methods to build a CQL3 table
 */
public final class TableBuilder {

    private TableBuilder() {
    }

    /**
     * Returns a {@link com.englishtown.vertx.cassandra.tablebuilder.CreateTable} builder
     *
     * @param keyspace the keyspace for the table to create
     * @param table    the table name
     * @return the create table builder
     */
    public static CreateTable create(String keyspace, String table) {
        return new CreateTable(keyspace, table);
    }

    /**
     * Returns a {@link com.englishtown.vertx.cassandra.tablebuilder.AlterTable} builder
     *
     * @param keyspace the keyspace for the table to create
     * @param table    the table name
     * @return the create table builder
     */
    public static AlterTable alter(String keyspace, String table) {
        return new AlterTable(keyspace, table);
    }

    /**
     * Returns a {@link com.datastax.driver.core.BatchStatement} with all the alter statements necessary to modify an existing table.
     * <p/>
     * Note: Columns will only be added or modified, not dropped.
     *
     * @param existing the existing table to be modified
     * @param desired  the desired end result
     * @return a set of statements to modify an existing table
     */
    public static BatchStatement alter(TableMetadata existing, CreateTable desired) {
        BatchStatement batch = new BatchStatement();

        for (BuiltTableStatement.Column column : desired.getColumns()) {
            ColumnMetadata columnMetadata = existing.getColumn(column.getName());
            if (columnMetadata == null) {
                batch.add(alter(desired.getKeyspace(), desired.getTable()).addColumn(column.getName(), column.getType()));
            } else if (!columnMetadata.getType().toString().equalsIgnoreCase(column.getType())) {
                if (columnMetadata.isStatic()) {
                    throw new IllegalArgumentException("A static column cannot have its type modified");
                }
                batch.add(alter(desired.getKeyspace(), desired.getTable()).alterColumn(column.getName(), column.getType()));
            }
        }

        return batch;
    }

    /**
     * Returns a {@link com.englishtown.vertx.cassandra.tablebuilder.DropTable} statement
     *
     * @param keyspace the keyspace for the table to create
     * @param table    the table name
     * @return the drop table builder
     */
    public static DropTable drop(String keyspace, String table) {
        return new DropTable(keyspace, table);
    }

}
