package com.englishtown.vertx.cassandra.integration.hk2;

import com.englishtown.vertx.cassandra.integration.CassandraSessionIntegrationTest;
import com.englishtown.vertx.cassandra.integration.Locator;

/**
 * HK2 version of {@link CassandraSessionIntegrationTest}
 */
public class HK2CassandraSessionIntegrationTest extends CassandraSessionIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new HK2Locator(vertx);
    }

}
