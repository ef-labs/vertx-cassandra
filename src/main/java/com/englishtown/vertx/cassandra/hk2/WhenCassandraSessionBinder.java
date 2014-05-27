package com.englishtown.vertx.cassandra.hk2;

import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 * HK2 Binder for when.java cassandra sessions
 */
public class WhenCassandraSessionBinder extends AbstractBinder {
    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        install(new CassandraSessionBinder());

        bind(DefaultWhenCassandraSession.class).to(WhenCassandraSession.class).in(Singleton.class);

    }
}
