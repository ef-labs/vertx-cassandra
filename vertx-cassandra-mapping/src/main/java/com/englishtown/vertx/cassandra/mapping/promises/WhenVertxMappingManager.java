package com.englishtown.vertx.cassandra.mapping.promises;

import com.datastax.driver.mapping.MappingManager;
import com.englishtown.vertx.cassandra.mapping.VertxMappingManager;

/**
 * When.java wrapper over {@link VertxMappingManager}
 */
public interface WhenVertxMappingManager {

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
    <T> WhenVertxMapper<T> mapper(Class<T> klass);


}
