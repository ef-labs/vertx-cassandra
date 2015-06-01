package com.englishtown.vertx.cassandra.integration.hk2;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.WhenCassandraSessionIntegrationTest;

/**
 * HK2 version of {@link WhenCassandraSessionIntegrationTest}
 */
public class HK2WhenCassandraSessionIntegrationTest extends WhenCassandraSessionIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new HK2Locator(vertx);
    }

}
