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

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.List;

/**
 * Default implementation of {@link CassandraSession}
 */
public class DefaultCassandraSession implements CassandraSession {

    private final Provider<Cluster.Builder> builderProvider;
    private final Vertx vertx;

    protected Cluster cluster;
    protected Session session;
    protected Metrics metrics;
    protected CassandraConfigurator configurator;


    @Inject
    public DefaultCassandraSession(Provider<Cluster.Builder> builderProvider, CassandraConfigurator configurator, Vertx vertx) {
        this.builderProvider = builderProvider;
        this.configurator = configurator;
        this.vertx = vertx;
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

        Cluster.Builder builder = builderProvider.get();

        // Add cassandra cluster contact points
        for (String seed : seeds) {
            builder.addContactPoint(seed);
        }

        // Add policies to cluster builder
        if (configurator.getLoadBalancingPolicy() != null) {
            builder.withLoadBalancingPolicy(configurator.getLoadBalancingPolicy());
        }

        // Add pooling options to cluster builder
        if (configurator.getPoolingOptions() != null) {
            builder.withPoolingOptions(configurator.getPoolingOptions());
        }

        // Add socket options to cluster builder
        if (configurator.getSocketOptions() != null) {
            builder.withSocketOptions(configurator.getSocketOptions());
        }

        if (configurator.getQueryOptions() != null) {
            builder.withQueryOptions(configurator.getQueryOptions());
        }

        if (configurator.getMetricsOptions() != null) {
            if (!configurator.getMetricsOptions().isJMXReportingEnabled()) {
                builder.withoutJMXReporting();
            }
        }

        // Build cluster and metrics
        cluster = builder.build();
        metrics = new Metrics(this);

        // Connect
        session = cluster.connect();

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
        return cluster.getMetadata();
    }

    @Override
    public boolean isClosed() {
        return (session == null ? true : session.isClosed());
    }

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    @Override
    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
            session = null;
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }
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