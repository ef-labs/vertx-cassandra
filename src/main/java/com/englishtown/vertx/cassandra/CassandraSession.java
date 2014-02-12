package com.englishtown.vertx.cassandra;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.FutureCallback;

/**
 * Interface that represents a cassandra session
 */
public interface CassandraSession extends AutoCloseable {

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
     * Executes a cassandra statement synchronously
     * <p/>
     * Use with caution as this will block!
     *
     * @param statement the statement to execute
     */
    ResultSet execute(Statement statement);

    /**
     * Executes a cassandra CQL query synchronously
     * <p/>
     * Use with caution as this will block!
     *
     * @param query the CQL query to execute
     */
    ResultSet execute(String query);

    /**
     * Returns cassandra metadata
     *
     * @return returns the cassandra metadata for the current session
     */
    Metadata getMetadata();

}
