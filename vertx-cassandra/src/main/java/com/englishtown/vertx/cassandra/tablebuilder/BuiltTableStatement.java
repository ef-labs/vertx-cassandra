package com.englishtown.vertx.cassandra.tablebuilder;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;

import java.nio.ByteBuffer;

/**
 * Builds a CQL3 statement to manipulate tables
 */
public abstract class BuiltTableStatement extends RegularStatement {

    protected String keyspace;
    protected String table;

    protected BuiltTableStatement(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    /**
     * Builds the CQL3 statement
     *
     * @return a {@link java.lang.StringBuilder} of the CQL statement
     */
    public abstract StringBuilder buildQueryString();

    /**
     * {@inheritDoc}
     */
    @Override
    public String getQueryString() {
        return buildQueryString().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasValues() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getRoutingKey() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Returns the table name
     *
     * @return the table name
     */
    public String getTable() {
        return table;
    }

    /**
     * A CQL3 column
     */
    public static class Column {

        protected Column(String name, String type, boolean isStatic) {
            this.name = name;
            this.type = type;
            this.isStatic = isStatic;
        }

        private String name;
        private String type;
        private boolean isStatic;

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public boolean getIsStatic() {
            return isStatic;
        }

    }

}
