package com.englishtown.vertx.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.google.common.util.concurrent.FutureCallback;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Interface that represents a cassandra session
 */
public interface CassandraSession extends AsyncCqlSession, AutoCloseable {

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @param callback  the callback for on completion
     */
    void executeAsync(Statement<?> statement, final FutureCallback<AsyncResultSet> callback);

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query    the CQL query to execute
     * @param callback the callback for on completion
     */
    void executeAsync(String query, final FutureCallback<AsyncResultSet> callback);

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @param callback  the callback for on completion
     */
    void prepareAsync(SimpleStatement statement, FutureCallback<PreparedStatement> callback);

    /**
     * Prepares the provided query
     *
     * @param query    the query to prepare
     * @param callback the callback for on completion
     */
    void prepareAsync(String query, FutureCallback<PreparedStatement> callback);

    /**
     * Flag to indicate if the session is initialized and ready to use
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
     * Returns current the underlying DataStax {@link CqlSession}
     *
     * @return
     */
    CqlSession getSession();

    /**
     * Returns the vert.x instance
     *
     * @return
     */
    Vertx getVertx();

}
