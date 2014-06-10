package com.englishtown.vertx.cassandra.keyspacebuilder;

/**
 * DROP KEYSPACE CQL3 statement builder
 */
public class DropKeyspace extends BuiltKeyspaceStatement {

    private boolean ifExists;

    DropKeyspace(String keyspace) {
        super(keyspace);
    }

    public DropKeyspace ifExists() {
        ifExists = true;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuilder buildQueryString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP KEYSPACE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(keyspace);

        return sb;
    }
}
