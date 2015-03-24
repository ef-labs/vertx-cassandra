package com.englishtown.vertx.cassandra.mapping.guice;

import com.englishtown.vertx.cassandra.mapping.VertxMappingManager;
import com.englishtown.vertx.cassandra.mapping.impl.DefaultVertxMappingManager;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

/**
 * Guice binder for cassandra mapping
 */
public class GuiceMappingBinder extends AbstractModule {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {

        bind(VertxMappingManager.class).to(DefaultVertxMappingManager.class).in(Singleton.class);

    }
}
