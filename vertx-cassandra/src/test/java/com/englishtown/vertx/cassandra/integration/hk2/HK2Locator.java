package com.englishtown.vertx.cassandra.integration.hk2;

import com.englishtown.vertx.cassandra.hk2.HK2WhenCassandraBinder;
import com.englishtown.vertx.cassandra.integration.Locator;
import com.englishtown.vertx.hk2.HK2VertxBinder;
import com.englishtown.vertx.promises.hk2.HK2WhenBinder;
import io.vertx.core.Vertx;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.ServiceLocatorFactory;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;

/**
 * HK2 {@link Locator} implementation
 */
public class HK2Locator implements Locator {

    private ServiceLocator locator;

    public HK2Locator(Vertx vertx, Binder... binders) {

        locator = ServiceLocatorFactory.getInstance().create(null);
        ServiceLocatorUtilities.bind(locator,
                new HK2WhenBinder(),
                new HK2WhenCassandraBinder(),
                new HK2VertxBinder(vertx));

        if (binders != null) {
            ServiceLocatorUtilities.bind(locator, binders);
        }

    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return locator.getService(clazz);
    }
}
