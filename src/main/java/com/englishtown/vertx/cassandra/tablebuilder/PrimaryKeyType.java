package com.englishtown.vertx.cassandra.tablebuilder;

/**
 * CQL3 Primary Key types
 */
public enum PrimaryKeyType {
    /**
     * A partitioning primary key
     * Useful for composite partitioning primary keys
     */
    PARTITIONING,
    /**
     * A clustering primary key used in compound primary keys
     */
    CLUSTERING
}
