package com.englishtown.vertx.cassandra.promises;

import com.datastax.driver.core.*;
import com.englishtown.promises.Promise;
import com.englishtown.vertx.cassandra.CassandraSession;

/**
 * When.java wrapper over {@link com.datastax.driver.core.Session}
 */
public interface WhenCassandraSession extends AutoCloseable {

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @return the promise for the {@link com.datastax.driver.core.ResultSet}
     */
    Promise<ResultSet> executeAsync(Statement statement);

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query the CQL query to execute
     * @return the promise for the {@link com.datastax.driver.core.ResultSet}
     */
    Promise<ResultSet> executeAsync(String query);

    /**
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query, values))}.
     *
     * @param query
     * @param values
     * @return
     */
    Promise<ResultSet> executeAsync(String query, Object... values);

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @return the promise for the {@link com.datastax.driver.core.PreparedStatement}
     */
    Promise<PreparedStatement> prepareAsync(RegularStatement statement);

    /**
     * Prepares the provided query
     *
     * @param query the query to prepare
     * @return the promise for the {@link com.datastax.driver.core.PreparedStatement}
     */
    Promise<PreparedStatement> prepareAsync(String query);

    /**
     * Returns cassandra metadata
     *
     * @return returns the cassandra metadata for the current session
     */
    Metadata getMetadata();

    /**
     * Whether this Session instance has been closed.
     *
     * @return {@code true} if this Session instance has been closed, {@code false}
     * otherwise.
     */
    boolean isClosed();

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    Cluster getCluster();

    /**
     * Return the {@link com.englishtown.vertx.cassandra.CassandraSession}
     *
     * @return
     */
    CassandraSession getSession();

    /**
     * Promise for when the session is ready
     *
     * @return
     */
    Promise<Void> ready();

}
