package com.englishtown.vertx.cassandra;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Provides cassandra configuration for the session
 */
public interface CassandraConfigurator {

    /**
     * Register a callback for when the configurator is ready to use
     *
     * @param callback
     */
    void onReady(Handler<AsyncResult<CqlSessionBuilder>> callback);

}
