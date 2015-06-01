package com.englishtown.vertx.cassandra.integration.guice;

import com.englishtown.vertx.cassandra.integration.CassandraSessionIntegrationTest;
import com.englishtown.vertx.cassandra.integration.Locator;

/**
 * Guice version of {@link CassandraSessionIntegrationTest}
 */
public class GuiceCassandraSessionIntegrationTest extends CassandraSessionIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new GuiceLocator(vertx);
    }

}
