package com.englishtown.vertx.cassandra.promises.impl;

import com.datastax.driver.core.*;
import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.FutureUtils;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Vertx;

import javax.inject.Inject;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.promises.WhenCassandraSession}
 */
public class DefaultWhenCassandraSession implements WhenCassandraSession {

    private final CassandraSession session;
    private final When when;
    private final Vertx vertx;

    @Inject
    public DefaultWhenCassandraSession(CassandraSession session, When when, Vertx vertx) {
        this.session = session;
        this.when = when;
        this.vertx = vertx;
    }

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @return the promise for the {@link com.datastax.driver.core.ResultSet}
     */
    @Override

    public Promise<ResultSet> executeAsync(Statement statement) {
        return convertFuture(session.executeAsync(statement));
    }

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query the CQL query to execute
     * @return the promise for the {@link com.datastax.driver.core.ResultSet}
     */
    @Override
    public Promise<ResultSet> executeAsync(String query) {
        return convertFuture(session.executeAsync(query));
    }

    /**
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query, values))}.
     *
     * @param query
     * @param values
     * @return
     */
    @Override
    public Promise<ResultSet> executeAsync(String query, Object... values) {
        return convertFuture(session.executeAsync(query, values));
    }

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @return the promise for the {@link com.datastax.driver.core.PreparedStatement}
     */
    @Override
    public Promise<PreparedStatement> prepareAsync(RegularStatement statement) {
        return convertFuture(session.prepareAsync(statement));
    }

    /**
     * Prepares the provided query
     *
     * @param query the query to prepare
     * @return the promise for the {@link com.datastax.driver.core.PreparedStatement}
     */
    @Override
    public Promise<PreparedStatement> prepareAsync(String query) {
        return convertFuture(session.prepareAsync(query));
    }

    /**
     * Returns cassandra metadata
     *
     * @return returns the cassandra metadata for the current session
     */
    @Override
    public Metadata getMetadata() {
        return session.getMetadata();
    }

    /**
     * Whether this Session instance has been closed.
     *
     * @return {@code true} if this Session instance has been closed, {@code false}
     * otherwise.
     */
    @Override
    public boolean isClosed() {
        return session.isClosed();
    }

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    @Override
    public Cluster getCluster() {
        return session.getCluster();
    }

    /**
     * Return the {@link com.englishtown.vertx.cassandra.CassandraSession}
     *
     * @return
     */
    @Override
    public CassandraSession getSession() {
        return session;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        session.close();
    }

    private <T> Promise<T> convertFuture(ListenableFuture<T> future) {

        Deferred<T> d = when.defer();

        FutureCallback<T> callback = new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                d.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                d.reject(t);
            }
        };

        FutureUtils.addCallback(future, callback, vertx);
        return d.getPromise();

    }

}
