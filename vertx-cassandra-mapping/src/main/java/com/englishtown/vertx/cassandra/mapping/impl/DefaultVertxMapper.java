package com.englishtown.vertx.cassandra.mapping.impl;

import com.datastax.driver.mapping.Mapper;
import com.englishtown.vertx.cassandra.FutureUtils;
import com.englishtown.vertx.cassandra.mapping.VertxMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Vertx;

/**
 * Default implementation of {@link VertxMapper}
 */
public class DefaultVertxMapper<T> implements VertxMapper<T> {

    private final Mapper<T> mapper;
    private final Vertx vertx;

    public DefaultVertxMapper(Mapper<T> mapper, Vertx vertx) {
        this.mapper = mapper;
        this.vertx = vertx;
    }

    @Override
    public Mapper<T> getMapper() {
        return mapper;
    }

    @Override
    public void saveAsync(T entity, FutureCallback<Void> callback) {
        ListenableFuture<Void> future = mapper.saveAsync(entity);
        FutureUtils.addCallback(future, callback, vertx);
    }

    @Override
    public void deleteAsync(T entity, FutureCallback<Void> callback) {
        ListenableFuture<Void> future = mapper.deleteAsync(entity);
        FutureUtils.addCallback(future, callback, vertx);
    }

    @Override
    public void deleteAsync(FutureCallback<Void> callback, Object... primaryKey) {
        ListenableFuture<Void> future = mapper.deleteAsync(primaryKey);
        FutureUtils.addCallback(future, callback, vertx);
    }

    @Override
    public void getAsync(FutureCallback<T> callback, Object... primaryKey) {
        ListenableFuture<T> future = mapper.getAsync(primaryKey);
        FutureUtils.addCallback(future, callback, vertx);
    }
}
