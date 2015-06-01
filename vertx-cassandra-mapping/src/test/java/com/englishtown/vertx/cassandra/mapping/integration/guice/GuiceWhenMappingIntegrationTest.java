package com.englishtown.vertx.cassandra.mapping.integration.guice;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.guice.GuiceLocator;
import com.englishtown.vertx.cassandra.mapping.guice.WhenGuiceMappingBinder;
import com.englishtown.vertx.cassandra.mapping.integration.WhenMappingIntegrationTest;

/**
 * Guice version of {@link WhenMappingIntegrationTest}
 */
public class GuiceWhenMappingIntegrationTest extends WhenMappingIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new GuiceLocator(vertx, new WhenGuiceMappingBinder());
    }

}
