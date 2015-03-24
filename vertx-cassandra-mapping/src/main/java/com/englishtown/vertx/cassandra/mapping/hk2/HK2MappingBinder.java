package com.englishtown.vertx.cassandra.mapping.hk2;

import com.englishtown.vertx.cassandra.mapping.VertxMappingManager;
import com.englishtown.vertx.cassandra.mapping.impl.DefaultVertxMappingManager;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 binder for cassandra mapping
 */
public class HK2MappingBinder extends AbstractBinder {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {

        bind(DefaultVertxMappingManager.class).to(VertxMappingManager.class).in(Singleton.class);

    }
}
