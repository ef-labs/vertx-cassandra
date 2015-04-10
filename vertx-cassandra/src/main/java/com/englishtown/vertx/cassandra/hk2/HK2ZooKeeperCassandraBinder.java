package com.englishtown.vertx.cassandra.hk2;

import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.zookeeper.ZooKeeperCassandraConfigurator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 binder for zookeeper cassandra configuration
 */
public class HK2ZooKeeperCassandraBinder extends AbstractBinder {
    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        // Install main bindings
        install(new HK2CassandraBinder());

        // zookeeper bindings
        bind(ZooKeeperCassandraConfigurator.class).to(CassandraConfigurator.class).in(Singleton.class).ranked(10);

    }
}
