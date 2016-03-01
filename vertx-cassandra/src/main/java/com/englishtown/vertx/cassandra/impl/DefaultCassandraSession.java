package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.FutureUtils;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of {@link CassandraSession}
 */
public class DefaultCassandraSession implements CassandraSession {

    private Cluster.Builder clusterBuilder;
    private final Vertx vertx;
    private List<Handler<AsyncResult<Void>>> onReadyCallbacks = new ArrayList<>();

    protected Cluster cluster;
    protected Session session;
    protected Metrics metrics;
    protected CassandraConfigurator configurator;
    protected AsyncResult<Void> initResult;

    private final Logger logger = LoggerFactory.getLogger(DefaultCassandraSession.class);

    @Inject
    public DefaultCassandraSession(Cluster.Builder clusterBuilder, CassandraConfigurator configurator, Vertx vertx) {
        this.clusterBuilder = clusterBuilder;
        this.configurator = configurator;
        this.vertx = vertx;
        this.metrics = new Metrics(this);

        configurator.onReady(result -> {
            if (result.failed()) {
                runOnReadyCallbacks(result);
                return;
            }
            init(configurator);
        });
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

        if (configurator.getPort() != null) {
            clusterBuilder.withPort(configurator.getPort());
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

        runOnReadyCallbacks(Future.succeededFuture(null));
    }

    private void runOnReadyCallbacks(AsyncResult<Void> result) {
        initResult = result;
        onReadyCallbacks.forEach(handler -> handler.handle(result));
        onReadyCallbacks.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metadata getMetadata() {
        Cluster cluster = getCluster();
        return cluster == null ? null : cluster.getMetadata();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return (session == null || session.isClosed());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster getCluster() {
        return session == null ? null : session.getCluster();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public State getState() {
        return getSession().getState();
    }

    /**
     * {@inheritDoc}
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean initialized() {
        return initResult != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReady(Handler<AsyncResult<Void>> callback) {
        if (initResult != null) {
            callback.handle(initResult);
        } else {
            onReadyCallbacks.add(callback);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Session getSession() {
        checkInitialized();
        return session;
    }

    /**
     * Returns the vert.x instance
     *
     * @return
     */
    @Override
    public Vertx getVertx() {
        return vertx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLoggedKeyspace() {
        return getSession().getLoggedKeyspace();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Session init() {
        return getSession().init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query) {
        return getSession().execute(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query, Object... values) {
        return getSession().execute(query, values);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(Statement statement) {
        return getSession().execute(statement);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query) {
        return getSession().executeAsync(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query, Object... values) {
        return getSession().executeAsync(query, values);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        return getSession().executeAsync(statement);
    }

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @param callback  the callback for on completion
     */
    @Override
    public void executeAsync(Statement statement, FutureCallback<ResultSet> callback) {
        addCallback(executeAsync(statement), callback);
    }

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query    the CQL query to execute
     * @param callback the callback for on completion
     */
    @Override
    public void executeAsync(String query, FutureCallback<ResultSet> callback) {
        addCallback(executeAsync(query), callback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepare(String query) {
        return getSession().prepare(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        return getSession().prepare(statement);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return getSession().prepareAsync(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
        return getSession().prepareAsync(statement);
    }

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @param callback  the callback for on completion
     */
    @Override
    public void prepareAsync(RegularStatement statement, FutureCallback<PreparedStatement> callback) {
        addCallback(prepareAsync(statement), callback);
    }

    /**
     * Prepares the provided query
     *
     * @param query    the query to prepare
     * @param callback the callback for on completion
     */
    @Override
    public void prepareAsync(String query, FutureCallback<PreparedStatement> callback) {
        addCallback(prepareAsync(query), callback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseFuture closeAsync() {
        return getSession().closeAsync();
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

    private <V> void addCallback(final ListenableFuture<V> future, FutureCallback<V> callback) {
        FutureUtils.addCallback(future, callback, vertx);
    }

    private void checkInitialized() {
        if (!initialized()) {
            throw new IllegalStateException("Cassandra session is not ready for use yet");
        }
    }

}