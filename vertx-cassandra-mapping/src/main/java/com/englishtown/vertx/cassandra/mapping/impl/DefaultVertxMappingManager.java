package com.englishtown.vertx.cassandra.mapping.impl;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.mapping.VertxMapper;
import com.englishtown.vertx.cassandra.mapping.VertxMappingManager;

import javax.inject.Inject;

/**
 * Default implementation of {@link VertxMappingManager}
 */
public class DefaultVertxMappingManager implements VertxMappingManager {

    private final CassandraSession session;
    private final MappingManager mappingManager;

    @Inject
    public DefaultVertxMappingManager(CassandraSession session) {
        this.session = session;
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
    public <T> VertxMapper<T> mapper(Class<T> klass) {
        Mapper<T> mapper = getMappingManager().mapper(klass);
        return new DefaultVertxMapper<>(mapper, session.getVertx());
    }
}
