package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

/**
 * Default implementation of {@link CassandraSession}
 */
public class DefaultCassandraSession implements CassandraSession {

    private Cluster.Builder clusterBuilder;
    private final Vertx vertx;

    protected Cluster cluster;
    protected Session session;
    protected Metrics metrics;
    protected CassandraConfigurator configurator;

    private final Logger logger = LoggerFactory.getLogger(DefaultCassandraSession.class);

    @Inject
    public DefaultCassandraSession(Cluster.Builder clusterBuilder, CassandraConfigurator configurator, Vertx vertx) {
        this.clusterBuilder = clusterBuilder;
        this.configurator = configurator;
        this.vertx = vertx;
        this.metrics = new Metrics(this);
        init(configurator);
    }

    CassandraConfigurator getConfigurator() {
        return configurator;
    }

    protected void init(CassandraConfigurator configurator) {

        // Get array of IPs, default to localhost
        List<String> seeds = configurator.getSeeds();
        if (seeds == null || seeds.isEmpty()) {
            throw new RuntimeException("Cassandra seeds are missing");
        }

        // Add cassandra cluster contact points
        for (String seed : seeds) {
            clusterBuilder.addContactPoint(seed);
        }

        // Add policies to cluster builder
        if (configurator.getLoadBalancingPolicy() != null) {
            clusterBuilder.withLoadBalancingPolicy(configurator.getLoadBalancingPolicy());
        }
        if (configurator.getReconnectionPolicy() != null) {
            clusterBuilder.withReconnectionPolicy(configurator.getReconnectionPolicy());
        }

        // Add pooling options to cluster builder
        if (configurator.getPoolingOptions() != null) {
            clusterBuilder.withPoolingOptions(configurator.getPoolingOptions());
        }

        // Add socket options to cluster builder
        if (configurator.getSocketOptions() != null) {
            clusterBuilder.withSocketOptions(configurator.getSocketOptions());
        }

        if (configurator.getQueryOptions() != null) {
            clusterBuilder.withQueryOptions(configurator.getQueryOptions());
        }

        if (configurator.getMetricsOptions() != null) {
            if (!configurator.getMetricsOptions().isJMXReportingEnabled()) {
                clusterBuilder.withoutJMXReporting();
            }
        }

        if (configurator.getAuthProvider() != null) {
            clusterBuilder.withAuthProvider(configurator.getAuthProvider());
        }

        // Build cluster and connect
        cluster = clusterBuilder.build();
        reconnect();

    }

    @Override
    public void executeAsync(Statement statement, FutureCallback<ResultSet> callback) {
        final ResultSetFuture future = session.executeAsync(statement);
        Futures.addCallback(future, wrapCallback(callback));
    }

    @Override
    public void executeAsync(String query, FutureCallback<ResultSet> callback) {
        executeAsync(new SimpleStatement(query), callback);
    }

    @Override
    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    @Override
    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

    @Override
    public void prepareAsync(RegularStatement statement, FutureCallback<PreparedStatement> callback) {
        ListenableFuture<PreparedStatement> future = session.prepareAsync(statement);
        Futures.addCallback(future, wrapCallback(callback));
    }

    @Override
    public void prepareAsync(String query, FutureCallback<PreparedStatement> callback) {
        ListenableFuture<PreparedStatement> future = session.prepareAsync(query);
        Futures.addCallback(future, wrapCallback(callback));
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        return session.prepare(statement);
    }

    @Override
    public PreparedStatement prepare(String query) {
        return session.prepare(query);
    }

    @Override
    public Metadata getMetadata() {
        Cluster cluster = getCluster();
        return cluster == null ? null : cluster.getMetadata();
    }

    @Override
    public boolean isClosed() {
        return (session == null || session.isClosed());
    }

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    @Override
    public Cluster getCluster() {
        return session == null ? null : session.getCluster();
    }

    /**
     * Reconnects to the cluster with a new session.  Any existing session is closed asynchronously.
     */
    @Override
    public void reconnect() {
        logger.debug("Call to reconnect the session has been made");
        Session oldSession = session;
        session = cluster.connect();
        if (oldSession != null) {
            oldSession.closeAsync();
        }
        metrics.afterReconnect();
    }

    @Override
    public void close() {
        logger.debug("Call to close the session has been made");
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }
        if (cluster != null) {
            cluster.closeAsync().force();
            cluster = null;
            session = null;
        }
        clusterBuilder = null;
    }

    private <V> FutureCallback<V> wrapCallback(final FutureCallback<V> callback) {
        final Context context = vertx.currentContext();

        // Make sure the callback runs on the vert.x thread
        return new FutureCallback<V>() {
            @Override
            public void onSuccess(final V result) {
                context.runOnContext(new Handler<Void>() {
                    @Override
                    public void handle(Void aVoid) {
                        callback.onSuccess(result);
                    }
                });
            }

            @Override
            public void onFailure(final Throwable t) {
                context.runOnContext(new Handler<Void>() {
                    @Override
                    public void handle(Void aVoid) {
                        callback.onFailure(t);
                    }
                });
            }
        };
    }

}