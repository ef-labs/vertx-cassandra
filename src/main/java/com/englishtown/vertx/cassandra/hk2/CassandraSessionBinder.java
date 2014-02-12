package com.englishtown.vertx.cassandra.hk2;

import com.datastax.driver.core.Cluster;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 injection binder
 */
public class CassandraSessionBinder extends AbstractBinder {
    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {
        bind(Cluster.Builder.class).to(Cluster.Builder.class);
        bind(DefaultCassandraSession.class).to(CassandraSession.class).in(Singleton.class);
        bind(JsonCassandraConfigurator.class).to(CassandraConfigurator.class).in(Singleton.class);
    }
}
