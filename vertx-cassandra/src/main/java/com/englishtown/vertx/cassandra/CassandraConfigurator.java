package com.englishtown.vertx.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Provides cassandra configuration for the session
 */
public interface CassandraConfigurator {

    /**
     * List of cassandra seed hosts or IPs
     *
     * @return
     */
    List<String> getSeeds();

    /**
     * Optional load balancing policy
     *
     * @return
     */
    LoadBalancingPolicy getLoadBalancingPolicy();

    /**
     * Optional reconnection policy
     *
     * @return
     */
    ReconnectionPolicy getReconnectionPolicy();

    /**
     * Optional pooling options
     *
     * @return
     */
    PoolingOptions getPoolingOptions();

    /**
     * Optional socket options
     *
     * @return
     */
    SocketOptions getSocketOptions();

    /**
     * Optional query options
     *
     * @return
     */
    QueryOptions getQueryOptions();

    /**
     * Optional metrics options
     *
     * @return
     */
    MetricsOptions getMetricsOptions();

    /**
     * Optional auth provider
     *
     * @return
     */
    AuthProvider getAuthProvider();

    /**
     * Register a callback for when the configurator is ready to use
     *
     * @param callback
     */
    void onReady(Handler<AsyncResult<Void>> callback);
}
