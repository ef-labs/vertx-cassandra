package com.englishtown.vertx.cassandra.mapping.integration.hk2;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.hk2.HK2Locator;
import com.englishtown.vertx.cassandra.mapping.hk2.WhenHK2MappingBinder;
import com.englishtown.vertx.cassandra.mapping.integration.WhenMappingIntegrationTest;

/**
 * HK2 version of {@link WhenMappingIntegrationTest}
 */
public class HK2WhenMappingIntegrationTest extends WhenMappingIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new HK2Locator(vertx, new WhenHK2MappingBinder());
    }

}
