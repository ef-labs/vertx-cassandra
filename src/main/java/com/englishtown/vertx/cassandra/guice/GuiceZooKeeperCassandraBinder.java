package com.englishtown.vertx.cassandra.guice;

import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.impl.ZooKeeperCassandraConfigurator;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

/**
 * Guice binder for zookeeper cassandra configuration
 */
public class GuiceZooKeeperCassandraBinder extends AbstractModule {
    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        // Install main bindings
        install(new GuiceCassandraBinder());

        // zookeeper bindings
        bind(CassandraConfigurator.class).to(ZooKeeperCassandraConfigurator.class).in(Singleton.class);

    }
}
