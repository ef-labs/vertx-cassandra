package com.englishtown.vertx.cassandra.integration.hk2;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.TableBuilderIntegrationTest;

/**
 * HK2 version of {@link TableBuilderIntegrationTest}
 */
public class HK2TableBuilderIntegrationTest extends TableBuilderIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new HK2Locator(vertx);
    }

}
