package com.englishtown.vertx.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Interface that represents a cassandra session
 */
public interface CassandraSession extends Session, AutoCloseable {

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @param callback  the callback for on completion
     */
    void executeAsync(Statement statement, final FutureCallback<ResultSet> callback);

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query    the CQL query to execute
     * @param callback the callback for on completion
     */
    void executeAsync(String query, final FutureCallback<ResultSet> callback);

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @param callback  the callback for on completion
     */
    void prepareAsync(RegularStatement statement, FutureCallback<PreparedStatement> callback);

    /**
     * Prepares the provided query
     *
     * @param query    the query to prepare
     * @param callback the callback for on completion
     */
    void prepareAsync(String query, FutureCallback<PreparedStatement> callback);

    /**
     * Returns cassandra metadata
     *
     * @return returns the cassandra metadata for the current session
     */
    Metadata getMetadata();

    /**
     * Reconnects to the cluster with a new session.  Any existing session is closed asynchronously.
     */
    void reconnect();

    /**
     * Flag to indicate if the the session is initialized and ready to use
     *
     * @return
     */
    boolean initialized();

    /**
     * Callback for when the session is initialized and ready
     *
     * @param callback
     */
    void onReady(Handler<AsyncResult<Void>> callback);

    /**
     * Returns current the underlying DataStax {@link com.datastax.driver.core.Session}
     *
     * @return
     */
    Session getSession();

    /**
     * Returns the vert.x instance
     *
     * @return
     */
    Vertx getVertx();

}
