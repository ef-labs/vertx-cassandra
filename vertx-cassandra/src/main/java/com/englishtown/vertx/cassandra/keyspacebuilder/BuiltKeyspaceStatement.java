package com.englishtown.vertx.cassandra.keyspacebuilder;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A built CQL3 keyspace statement
 */
public abstract class BuiltKeyspaceStatement extends RegularStatement {

    protected String keyspace;

    protected BuiltKeyspaceStatement(String keyspace) {
        this.keyspace = keyspace;
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
    public String getQueryString(CodecRegistry codecRegistry) {
        return getQueryString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
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
    public boolean hasValues(CodecRegistry codecRegistry) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
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
     * {@inheritDoc}
     */
    @Override
    public Map<String, ByteBuffer> getNamedValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean usesNamedValues() {
        return false;
    }

}
