package com.englishtown.vertx.cassandra.integration;

/**
 * Created by adriangonzalez on 5/29/15.
 */
public interface Locator {

    <T> T getInstance(Class<T> clazz);

}
