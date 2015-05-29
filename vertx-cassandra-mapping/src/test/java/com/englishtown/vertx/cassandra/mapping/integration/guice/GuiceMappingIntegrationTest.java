package com.englishtown.vertx.cassandra.mapping.integration.guice;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.guice.GuiceLocator;
import com.englishtown.vertx.cassandra.mapping.guice.GuiceMappingBinder;
import com.englishtown.vertx.cassandra.mapping.integration.MappingIntegrationTest;

/**
 * Guice version of {@link MappingIntegrationTest}
 */
public class GuiceMappingIntegrationTest extends MappingIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new GuiceLocator(vertx, new GuiceMappingBinder());
    }

}
