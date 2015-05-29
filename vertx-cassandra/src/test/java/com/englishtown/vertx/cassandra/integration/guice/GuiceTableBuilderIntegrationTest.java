package com.englishtown.vertx.cassandra.integration.guice;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.TableBuilderIntegrationTest;

/**
 * Guice version of {@link TableBuilderIntegrationTest}
 */
public class GuiceTableBuilderIntegrationTest extends TableBuilderIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new GuiceLocator(vertx);
    }

}
