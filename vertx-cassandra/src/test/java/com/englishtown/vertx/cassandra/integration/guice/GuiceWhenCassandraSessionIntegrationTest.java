package com.englishtown.vertx.cassandra.integration.guice;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.WhenCassandraSessionIntegrationTest;

/**
 * Guice version of {@link WhenCassandraSessionIntegrationTest}
 */
public class GuiceWhenCassandraSessionIntegrationTest extends WhenCassandraSessionIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new GuiceLocator(vertx);
    }

}
