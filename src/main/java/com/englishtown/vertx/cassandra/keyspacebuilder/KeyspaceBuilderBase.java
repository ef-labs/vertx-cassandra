package com.englishtown.vertx.cassandra.keyspacebuilder;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for CREATE/ALTER KEYSPACE builders
 */
abstract class KeyspaceBuilderBase extends BuiltKeyspaceStatement {

    private String strategy = REPLICATION_STRATEGY_SIMPLE;
    private int replicationFactor = 1;
    private Map<String, Integer> dcReplicationFactors = new LinkedHashMap<>();

    public static final String REPLICATION_STRATEGY_SIMPLE = "SimpleStrategy";
    public static final String REPLICATION_STRATEGY_NETWORK_TOPOLOGY = "NetworkTopologyStrategy";

    protected KeyspaceBuilderBase(String keyspace) {
        super(keyspace);
    }

    public KeyspaceBuilderBase simpleStrategy(int replicationFactor) {
        strategy = REPLICATION_STRATEGY_SIMPLE;
        this.replicationFactor = replicationFactor;
        return this;
    }

    public Datacenter networkTopologyStrategy() {
        strategy = REPLICATION_STRATEGY_NETWORK_TOPOLOGY;
        return new Datacenter(keyspace);
    }

    protected StringBuilder buildReplicationStrategy(StringBuilder sb) {
        sb.append(" WITH REPLICATION = { 'class' : '");

        if (REPLICATION_STRATEGY_SIMPLE.equals(strategy)) {
            sb.append(REPLICATION_STRATEGY_SIMPLE)
                    .append("', 'replication_factor' : ")
                    .append(replicationFactor);
        } else {
            sb.append(REPLICATION_STRATEGY_NETWORK_TOPOLOGY)
                    .append("'");
            for (Map.Entry<String, Integer> entry : dcReplicationFactors.entrySet()) {
                sb.append(", '")
                        .append(entry.getKey())
                        .append("' : ")
                        .append(entry.getValue());
            }
        }

        return sb;
    }

    public class Datacenter extends BuiltKeyspaceStatement {

        private Datacenter(String keyspace) {
            super(keyspace);
        }

        public Datacenter dc(String name, int replicationFactor) {
            dcReplicationFactors.put(name, replicationFactor);
            return this;
        }

        /**
         * Builds the CQL3 statement
         *
         * @return a {@link StringBuilder} of the CQL statement
         */
        @Override
        public StringBuilder buildQueryString() {
            return KeyspaceBuilderBase.this.buildQueryString();
        }
    }
}
