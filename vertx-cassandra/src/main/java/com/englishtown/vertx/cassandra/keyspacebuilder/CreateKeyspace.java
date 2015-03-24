package com.englishtown.vertx.cassandra.keyspacebuilder;

/**
 * CREATE KEYSPACE CQL3 statement builder
 */
public class CreateKeyspace extends KeyspaceBuilderBase {

    private boolean ifNotExists;

    CreateKeyspace(String keyspace) {
        super(keyspace);
    }

    public CreateKeyspace ifNotExists() {
        ifNotExists = true;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuilder buildQueryString() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE KEYSPACE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(keyspace);
        buildReplicationStrategy(sb);
        sb.append(" }");
        return sb;
    }

}
