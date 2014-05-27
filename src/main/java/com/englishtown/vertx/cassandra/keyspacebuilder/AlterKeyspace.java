package com.englishtown.vertx.cassandra.keyspacebuilder;

/**
 * ALTER KEYSPACE CQL3 statement builder
 */
public class AlterKeyspace extends KeyspaceBuilderBase {

    AlterKeyspace(String keyspace) {
        super(keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuilder buildQueryString() {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER KEYSPACE ").append(keyspace);
        buildReplicationStrategy(sb);
        sb.append(" }");

        return sb;
    }
}
