package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
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
    protected Metadata metadata;
    protected boolean initialized;
    protected ConsistencyLevel consistency;


    @Inject
    public DefaultCassandraSession(Provider<Cluster.Builder> builderProvider, Vertx vertx) {
        this.builderProvider = builderProvider;
        this.vertx = vertx;
    }

    @Override
    public void init(CassandraConfigurator configurator) {

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

        // Build cluster and session
        cluster = builder.build();
        session = cluster.connect();
        metadata = cluster.getMetadata();

        consistency = configurator.getConsistency();
        initialized = true;
    }

    @Override
    public void executeAsync(Statement statement, FutureCallback<ResultSet> callback) {
        checkInitialized();
        if (consistency != null && statement.getConsistencyLevel() == null) {
            statement.setConsistencyLevel(consistency);
        }
        final ResultSetFuture future = session.executeAsync(statement);
        Futures.addCallback(future, wrapCallback(callback));
    }

    @Override
    public void executeAsync(String query, FutureCallback<ResultSet> callback) {
        executeAsync(new SimpleStatement(query), callback);
    }

    @Override
    public ResultSet execute(Statement statement) {
        checkInitialized();
        if (consistency != null && statement.getConsistencyLevel() == null) {
            statement.setConsistencyLevel(consistency);
        }
        return session.execute(statement);
    }

    @Override
    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

    @Override
    public Metadata getMetadata() {
        checkInitialized();
        return metadata;
    }

    @Override
    public void close() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    protected void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("The DefaultCassandraSession has not been initialized.");
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