package com.englishtown.vertx.cassandra.mapping;

import com.datastax.driver.mapping.MappingManager;

/**
 * Vert.x wrapper over a datastax {@link com.datastax.driver.mapping.MappingManager}
 */
public interface VertxMappingManager {

    /**
     * Gets the underlying datastax {@link MappingManager}
     *
     * @return
     */
    MappingManager getMappingManager();

    /**
     * Returns a vert.x wrapped {@link com.datastax.driver.mapping.Mapper}
     *
     * @param klass
     * @param <T>
     * @return
     */
    <T> VertxMapper<T> mapper(Class<T> klass);

}
