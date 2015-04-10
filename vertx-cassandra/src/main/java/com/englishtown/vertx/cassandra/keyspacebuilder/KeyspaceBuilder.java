package com.englishtown.vertx.cassandra.keyspacebuilder;

/**
 * Static methods to build a CQL3 keyspace statement
 */
public class KeyspaceBuilder {

    /**
     * Returns a {@link com.englishtown.vertx.cassandra.keyspacebuilder.CreateKeyspace} builder
     *
     * @param keyspace the keyspace to be created
     * @return the create keyspace builder
     */
    public static CreateKeyspace create(String keyspace) {
        return new CreateKeyspace(keyspace);
    }

    /**
     * Returns a {@link com.englishtown.vertx.cassandra.keyspacebuilder.AlterKeyspace} builder
     *
     * @param keyspace the keyspace to be altered
     * @return the alter keyspace builder
     */
    public static AlterKeyspace alter(String keyspace) {
        return new AlterKeyspace(keyspace);
    }

    /**
     * Returns a {@link com.englishtown.vertx.cassandra.keyspacebuilder.DropKeyspace} builder
     *
     * @param keyspace the keyspace to drop
     * @return the drop keyspace builder
     */
    public static DropKeyspace drop(String keyspace) {
        return new DropKeyspace(keyspace);
    }

}
