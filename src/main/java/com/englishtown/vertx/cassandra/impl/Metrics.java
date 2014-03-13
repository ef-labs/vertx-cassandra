package com.englishtown.vertx.cassandra.impl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.englishtown.vertx.cassandra.CassandraConfigurator;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Metrics container
 */
class Metrics implements AutoCloseable {

    private MetricRegistry registry = new MetricRegistry();
    private JmxReporter reporter;

    public Metrics(DefaultCassandraSession session) {
        init(session);
    }

    protected void init(DefaultCassandraSession session) {

        CassandraConfigurator configurator = session.getConfigurator();

        // Add seed gauges
        List<String> seeds = configurator.getSeeds();
        if (seeds != null) {
            for (int i = 0; i < seeds.size(); i++) {
                final String seed = seeds.get(i);

                registry.register(name("initial-seed", String.valueOf(i)), new Gauge<String>() {
                    @Override
                    public String getValue() {
                        return seed;
                    }
                });
            }
        }

        final ConsistencyLevel consistency = configurator.getConsistency();
        registry.register("consistency", new Gauge<String>() {
            @Override
            public String getValue() {
                return (consistency == null ? null : consistency.name());
            }
        });

        // Add load balancing gauges
        final LoadBalancingPolicy lbPolicy = configurator.getLoadBalancingPolicy();
        registry.register("lb-policy", new Gauge<String>() {
            @Override
            public String getValue() {
                return (lbPolicy == null ? null : lbPolicy.getClass().getSimpleName());
            }
        });

        final Cluster cluster = session.getCluster();

        registry.register("closed", new Gauge<Boolean>() {
            @Override
            public Boolean getValue() {
                return cluster.isClosed();
            }
        });

        cluster.register(new GaugeStateListener());

        if (configurator.isJmxReportingEnabled()) {
            String domain = "et.cass." + cluster.getClusterName() + "-metrics";
            reporter = JmxReporter
                    .forRegistry(registry)
                    .inDomain(domain)
                    .build();

            reporter.start();
        }

    }

    @Override
    public void close() throws Exception {
        if (reporter != null) {
            reporter.stop();
        }
    }

    private class GaugeStateListener implements Host.StateListener {

        private final ConcurrentMap<String, Host> addedHosts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Host> upHosts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Host> removedHosts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Host> downHosts = new ConcurrentHashMap<>();

        public GaugeStateListener() {

            registry.register("added-hosts", new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(addedHosts);
                }
            });

            registry.register("up-hosts", new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(upHosts);
                }
            });

            registry.register("down-hosts", new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(downHosts);
                }
            });

            registry.register("removed-hosts", new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(removedHosts);
                }
            });

        }

        private String stringify(ConcurrentMap<String, Host> hosts) {

            StringBuilder sb = new StringBuilder();
            String delimiter = "";

            for (String key : hosts.keySet()) {
                Host host = hosts.get(key);
                if (host != null) {
                    sb.append(delimiter)
                            .append(host.toString())
                            .append(" (dc=")
                            .append(host.getDatacenter())
                            .append(" up=")
                            .append(host.isUp())
                            .append(")");

                    delimiter = "\n";
                }
            }

            return sb.toString();
        }

        private String getKey(Host host) {
            return host.getAddress().toString();
        }

        /**
         * Called when a new node is added to the cluster.
         * <p/>
         * The newly added node should be considered up.
         *
         * @param host the host that has been newly added.
         */
        @Override
        public void onAdd(Host host) {
            String key = getKey(host);
            addedHosts.put(key, host);
            removedHosts.remove(key);
        }

        /**
         * Called when a node is determined to be up.
         *
         * @param host the host that has been detected up.
         */
        @Override
        public void onUp(Host host) {
            String key = getKey(host);
            upHosts.put(key, host);
            downHosts.remove(key);
        }

        /**
         * Called when a node is determined to be down.
         *
         * @param host the host that has been detected down.
         */
        @Override
        public void onDown(Host host) {
            String key = getKey(host);
            downHosts.put(key, host);
            upHosts.remove(key);
        }

        /**
         * Called when a node is removed from the cluster.
         *
         * @param host the removed host.
         */
        @Override
        public void onRemove(Host host) {
            String key = getKey(host);
            removedHosts.put(key, host);
            addedHosts.remove(key);
        }
    }
}
