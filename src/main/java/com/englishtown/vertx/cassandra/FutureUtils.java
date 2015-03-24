package com.englishtown.vertx.cassandra;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * Future vert.x utils
 */
public class FutureUtils {

    private FutureUtils() {
    }

    /**
     * Add a future callback to run on the vert.x context
     *
     * @param future listenable future to have the callback added to
     * @param callback the callback for the listenable future
     * @param vertx
     * @param <V>
     */
    public static <V> void addCallback(final ListenableFuture<V> future, FutureCallback<? super V> callback, Vertx vertx) {
        final Context context = vertx.getOrCreateContext();
        Futures.addCallback(future, callback, command -> context.runOnContext(aVoid -> command.run()));
    }

}
