package com.englishtown.vertx.cassandra.mapping.integration.hk2;

import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.cassandra.integration.hk2.HK2Locator;
import com.englishtown.vertx.cassandra.mapping.hk2.HK2MappingBinder;
import com.englishtown.vertx.cassandra.mapping.integration.MappingIntegrationTest;

/**
 * HK2 version of {@link MappingIntegrationTest}
 */
public class HK2MappingIntegrationTest extends MappingIntegrationTest {

    @Override
    protected Locator createLocator() {
        return new HK2Locator(vertx, new HK2MappingBinder());
    }

}
