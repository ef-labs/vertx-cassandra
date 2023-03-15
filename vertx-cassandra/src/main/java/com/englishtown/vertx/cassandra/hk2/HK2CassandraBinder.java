package com.englishtown.vertx.cassandra.hk2;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 injection binder
 */
public class HK2CassandraBinder extends AbstractBinder {
    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        bind(CqlSessionBuilder.class).to(CqlSessionBuilder.class);
        bind(DefaultCassandraSession.class).to(CassandraSession.class).in(Singleton.class);
        bind(EnvironmentCassandraConfigurator.class).to(CassandraConfigurator.class).in(Singleton.class);

    }
}
