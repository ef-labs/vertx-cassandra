package com.englishtown.vertx.cassandra.mapping.promises.impl;

import com.datastax.driver.mapping.Mapper;
import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.FutureUtils;
import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Vertx;

/**
 * Default implementation of {@link WhenVertxMapper}
 */
public class DefaultWhenVertxMapper<T> implements WhenVertxMapper<T> {

    private final Mapper<T> mapper;
    private final When when;
    private final Vertx vertx;

    public DefaultWhenVertxMapper(Mapper<T> mapper, When when, Vertx vertx) {
        this.mapper = mapper;
        this.when = when;
        this.vertx = vertx;
    }

    @Override
    public Mapper<T> getMapper() {
        return mapper;
    }

    @Override
    public Promise<Void> saveAsync(T entity) {
        return convertFuture(mapper.saveAsync(entity));
    }

    @Override
    public Promise<Void> deleteAsync(T entity) {
        return convertFuture(mapper.deleteAsync(entity));
    }

    @Override
    public Promise<Void> deleteAsync(Object... primaryKey) {
        return convertFuture(mapper.deleteAsync(primaryKey));
    }

    @Override
    public Promise<T> getAsync(Object... primaryKey) {
        return convertFuture(mapper.getAsync(primaryKey));
    }

    private <T> Promise<T> convertFuture(ListenableFuture<T> future) {

        Deferred<T> d = when.defer();

        FutureCallback<T> callback = new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                d.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                d.reject(t);
            }
        };

        FutureUtils.addCallback(future, callback, vertx);
        return d.getPromise();

    }

}
