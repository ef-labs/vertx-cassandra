package com.englishtown.vertx.cassandra.guice;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

/**
 * Guice injection bindings
 */
public class GuiceCassandraBinder extends AbstractModule {
    /**
     * Configures a {@link com.google.inject.Binder} via the exposed methods.
     */
    @Override
    protected void configure() {

        bind(CqlSessionBuilder.class);
        bind(CassandraSession.class).to(DefaultCassandraSession.class).in(Singleton.class);
        bind(CassandraConfigurator.class).to(EnvironmentCassandraConfigurator.class).in(Singleton.class);

    }
}
