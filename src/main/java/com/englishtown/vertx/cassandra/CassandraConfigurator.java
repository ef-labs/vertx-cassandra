package com.englishtown.vertx.cassandra;

import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

import java.util.List;

/**
 * Provides cassandra configuration for the session
 */
public interface CassandraConfigurator {

    List<String> getSeeds();

    LoadBalancingPolicy getLoadBalancingPolicy();

    ReconnectionPolicy getReconnectionPolicy();

    PoolingOptions getPoolingOptions();

    SocketOptions getSocketOptions();

    QueryOptions getQueryOptions();

    MetricsOptions getMetricsOptions();

}
