package com.englishtown.vertx.cassandra.mapping.promises.impl;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMapper;
import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMappingManager;

import javax.inject.Inject;

/**
 * Default implementation of {@link WhenVertxMappingManager}
 */
public class DefaultWhenVertxMappingManager implements WhenVertxMappingManager {

    private final CassandraSession session;
    private final When when;
    private final MappingManager mappingManager;

    @Inject
    public DefaultWhenVertxMappingManager(CassandraSession session, When when) {
        this.session = session;
        this.when = when;
        this.mappingManager = new MappingManager(session);
    }

    /**
     * Gets the underlying datastax {@link MappingManager}
     *
     * @return
     */
    @Override
    public MappingManager getMappingManager() {
        return mappingManager;
    }

    /**
     * Returns a vert.x wrapped {@link Mapper}
     *
     * @param klass
     * @return
     */
    @Override
    public <T> WhenVertxMapper<T> mapper(Class<T> klass) {
        Mapper<T> mapper = getMappingManager().mapper(klass);
        return new DefaultWhenVertxMapper<>(mapper, when, session.getVertx());
    }
}
