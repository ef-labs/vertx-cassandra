package com.englishtown.vertx.cassandra.promises.impl;

import com.datastax.driver.core.*;
import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.google.common.util.concurrent.FutureCallback;

import javax.inject.Inject;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.promises.WhenCassandraSession}
 */
public class DefaultWhenCassandraSession implements WhenCassandraSession {

    private final CassandraSession session;
    private final When when;

    @Inject
    public DefaultWhenCassandraSession(CassandraSession session, When when) {
        this.session = session;
        this.when = when;
    }

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @return the promise for the {@link com.datastax.driver.core.ResultSet}
     */
    @Override

    public Promise<ResultSet> executeAsync(Statement statement) {
        final Deferred<ResultSet> d = when.defer();

        session.executeAsync(statement, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                d.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                d.reject(t);
            }
        });

        return d.getPromise();
    }

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query the CQL query to execute
     * @return the promise for the {@link com.datastax.driver.core.ResultSet}
     */
    @Override
    public Promise<ResultSet> executeAsync(String query) {
        final Deferred<ResultSet> d = when.defer();

        session.executeAsync(query, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                d.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                d.reject(t);
            }
        });

        return d.getPromise();
    }

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @return the promise for the {@link com.datastax.driver.core.PreparedStatement}
     */
    @Override
    public Promise<PreparedStatement> prepareAsync(RegularStatement statement) {
        final Deferred<PreparedStatement> d = when.defer();

        session.prepareAsync(statement, new FutureCallback<PreparedStatement>() {
            @Override
            public void onSuccess(PreparedStatement result) {
                d.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                d.reject(t);
            }
        });

        return d.getPromise();
    }

    /**
     * Prepares the provided query
     *
     * @param query the query to prepare
     * @return the promise for the {@link com.datastax.driver.core.PreparedStatement}
     */
    @Override
    public Promise<PreparedStatement> prepareAsync(String query) {
        final Deferred<PreparedStatement> d = when.defer();

        session.prepareAsync(query, new FutureCallback<PreparedStatement>() {
            @Override
            public void onSuccess(PreparedStatement result) {
                d.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                d.reject(t);
            }
        });

        return d.getPromise();

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
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        session.close();
    }
}
