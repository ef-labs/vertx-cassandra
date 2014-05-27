package com.englishtown.vertx.cassandra.tablebuilder;

import java.util.*;

/**
 * CREATE TABLE CQL3 statement builder
 */
public class CreateTable extends BuiltTableStatement {


    private Map<String, Column> columns = new TreeMap<>();
    private boolean ifNotExists = false;
    private Set<String> primaryKeys = new LinkedHashSet<>();
    private Set<String> partitioningKeys = new LinkedHashSet<>();

    CreateTable(String keyspace, String table) {
        super(keyspace, table);
    }

    /**
     * Returns a collection of the columns to be created
     *
     * @return collection of columns to create
     */
    public Collection<Column> getColumns() {
        return columns.values();
    }

    /**
     * Adds the "IF NOT EXISTS" cql clause
     *
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.CreateTable}
     */
    public CreateTable ifNotExists() {
        ifNotExists = true;
        return this;
    }

    /**
     * Adds a column
     *
     * @param name the column name
     * @param type the column CQL3 type
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.BuiltTableStatement}
     */
    public CreateTable column(String name, String type) {
        return addColumn(new Column(name, type, false));
    }

    /**
     * Adds a column
     *
     * @param name the column name
     * @param type the column CQL3 type
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.BuiltTableStatement}
     */
    public CreateTable staticColumn(String name, String type) {
        return addColumn(new Column(name, type, true));
    }

    private CreateTable addColumn(Column column) {
        columns.put(column.getName(), column);
        return this;
    }

    /**
     * Adds multiple columns
     *
     * @param names the column names
     * @param types the column CQL3 types
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.BuiltTableStatement}
     */
    public CreateTable columns(String[] names, String[] types) {
        if (names.length != types.length) {
            throw new IllegalArgumentException(String.format("Got %d names but %d types", names.length, types.length));
        }
        for (int i = 0; i < names.length; i++) {
            column(names[i], types[i]);
        }
        return this;
    }

    /**
     * Sets one or more primary key columns
     *
     * @param column column name
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.CreateTable}
     */
    public CreateTable primaryKeys(String... column) {
        Collections.addAll(primaryKeys, column);
        return this;
    }

    /**
     * Sets a primary key
     *
     * @param column column name
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.CreateTable}
     */
    public CreateTable primaryKey(String column) {
        primaryKeys.add(column);
        return this;
    }

    /**
     * Sets a primary key and type.  This allows you to create a table with a composite partition key
     *
     * @param column column name
     * @param type   partitioning or clustering
     * @return the current {@link com.englishtown.vertx.cassandra.tablebuilder.CreateTable}
     */
    public CreateTable primaryKey(String column, PrimaryKeyType type) {
        switch (type) {
            case PARTITIONING:
                primaryKeys.remove(column);
                partitioningKeys.add(column);
                break;
            default:
                primaryKeys.add(column);
                break;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public StringBuilder buildQueryString() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        if (keyspace != null) {
            sb.append(keyspace).append(".");
        }
        sb.append(table).append(" (");

        String delimiter = "";

        // Add columns
        for (Column column : columns.values()) {
            sb.append(delimiter).append(column.getName()).append(" ").append(column.getType());
            if (column.getIsStatic()) {
                sb.append(" STATIC");
            }
            delimiter = ", ";
        }

        // Add primary keys
        sb.append(delimiter).append("PRIMARY KEY(");
        delimiter = "";
        if (!partitioningKeys.isEmpty()) {
            sb.append("(");
            for (String column : partitioningKeys) {
                sb.append(delimiter).append(column);
                delimiter = ", ";
            }
            sb.append(")");
        }
        for (String column : primaryKeys) {
            sb.append(delimiter).append(column);
            delimiter = ", ";
        }
        sb.append(")");

        sb.append(")");

        return sb;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getQueryString();
    }

}
