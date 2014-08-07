package com.englishtown.vertx.cassandra.guice;

import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

/**
 * Guice injection bindings for when.java cassandra sessions
 */
public class GuiceWhenCassandraBinder extends AbstractModule {
    /**
     * Configures a {@link com.google.inject.Binder} via the exposed methods.
     */
    @Override
    protected void configure() {

        // Install main bindings
        install(new GuiceCassandraBinder());

        // when.java bindings
        bind(WhenCassandraSession.class).to(DefaultWhenCassandraSession.class).in(Singleton.class);

    }
}
