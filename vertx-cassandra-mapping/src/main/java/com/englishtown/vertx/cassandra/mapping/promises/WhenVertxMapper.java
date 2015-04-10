package com.englishtown.vertx.cassandra.mapping.promises;

import com.datastax.driver.mapping.Mapper;
import com.englishtown.promises.Promise;
import com.englishtown.vertx.cassandra.mapping.VertxMapper;

/**
 * When.java wrapper over {@link VertxMapper}
 */
public interface WhenVertxMapper<T> {

    Mapper<T> getMapper();

    Promise<Void> saveAsync(T entity);

    Promise<Void> deleteAsync(T entity);

    Promise<Void> deleteAsync(Object... primaryKey);

    Promise<T> getAsync(Object... primaryKey);

}
