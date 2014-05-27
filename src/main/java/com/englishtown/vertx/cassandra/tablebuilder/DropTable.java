package com.englishtown.vertx.cassandra.tablebuilder;

/**
 * DROP TABLE CQL3 statement builder
 */
public class DropTable extends BuiltTableStatement {

    private boolean ifExists = false;

    DropTable(String keyspace, String table) {
        super(keyspace, table);
    }

    public DropTable ifExists() {
        ifExists = true;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuilder buildQueryString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        if (keyspace != null) {
            sb.append(keyspace).append(".");
        }
        sb.append(table);

        return sb;
    }
}
