package com.englishtown.vertx.cassandra.mapping;

import com.datastax.driver.mapping.Mapper;
import com.google.common.util.concurrent.FutureCallback;

/**
 * Vert.x wrapper over a datastax {@link Mapper}
 */
public interface VertxMapper<T> {

    Mapper<T> getMapper();

    void saveAsync(T entity, FutureCallback<Void> callback);

    void deleteAsync(T entity, FutureCallback<Void> callback);

    void deleteAsync(FutureCallback<Void> callback, Object... primaryKey);

    void getAsync(FutureCallback<T> callback, Object... primaryKey);

}
