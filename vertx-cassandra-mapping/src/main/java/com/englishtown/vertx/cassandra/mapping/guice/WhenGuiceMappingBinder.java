package com.englishtown.vertx.cassandra.mapping.guice;

import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMappingManager;
import com.englishtown.vertx.cassandra.mapping.promises.impl.DefaultWhenVertxMappingManager;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

/**
 * Guice binder for when.java cassandra mapping
 */
public class WhenGuiceMappingBinder extends AbstractModule {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        install(new GuiceMappingBinder());

        bind(WhenVertxMappingManager.class).to(DefaultWhenVertxMappingManager.class).in(Singleton.class);
    }
}
