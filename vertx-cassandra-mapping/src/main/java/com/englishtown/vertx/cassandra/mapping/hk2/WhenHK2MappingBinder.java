package com.englishtown.vertx.cassandra.mapping.hk2;

import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMappingManager;
import com.englishtown.vertx.cassandra.mapping.promises.impl.DefaultWhenVertxMappingManager;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 binder for when.java cassandra mapping
 */
public class WhenHK2MappingBinder extends AbstractBinder {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        install(new HK2MappingBinder());

        bind(DefaultWhenVertxMappingManager.class).to(WhenVertxMappingManager.class).in(Singleton.class);
    }
}
