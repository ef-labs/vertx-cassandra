package com.englishtown.vertx.cassandra.hk2;

import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 Binder for when.java cassandra sessions
 */
public class HK2WhenCassandraBinder extends AbstractBinder {
    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        // Install main bindings
        install(new HK2CassandraBinder());

        // when.java bindings
        bind(DefaultWhenCassandraSession.class).to(WhenCassandraSession.class).in(Singleton.class);

    }
}
