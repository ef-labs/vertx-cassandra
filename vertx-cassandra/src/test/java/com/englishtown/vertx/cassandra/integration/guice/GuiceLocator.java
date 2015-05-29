package com.englishtown.vertx.cassandra.integration.guice;

import com.englishtown.vertx.cassandra.guice.GuiceWhenCassandraBinder;
import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.guice.GuiceVertxBinder;
import com.englishtown.vertx.promises.guice.GuiceWhenBinder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Guice {@link Locator} implementation
 */
public class GuiceLocator implements Locator {

    private Injector injector;

    public GuiceLocator(Vertx vertx, Module... modules) {

        List<Module> list = new ArrayList<>();
        list.add(new GuiceWhenBinder());
        list.add(new GuiceWhenCassandraBinder());
        list.add(new GuiceVertxBinder(vertx));

        if (modules != null) {
            Collections.addAll(list, modules);
        }

        injector = Guice.createInjector(list);

    }

    public Injector getInjector() {
        return injector;
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return injector.getInstance(clazz);
    }
}
