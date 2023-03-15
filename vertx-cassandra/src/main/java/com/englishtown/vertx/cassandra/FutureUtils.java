package com.englishtown.vertx.cassandra;

import com.google.common.util.concurrent.FutureCallback;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletionStage;

/**
 * Future vert.x utils
 */
public class FutureUtils {

    private FutureUtils() {
    }

    /**
     * Add a future callback to run on the vert.x context
     *
     * @param future   listenable future to have the callback added to
     * @param callback the callback for the listenable future
     * @param vertx
     * @param <V>
     */
    public static <V> void addCallback(final CompletionStage<V> future, FutureCallback<? super V> callback, Vertx vertx) {
        final Context context = vertx.getOrCreateContext();
        future.thenApply(result -> {
                    context.runOnContext(aVoid -> callback.onSuccess(result));
                    return null;
                })
                .exceptionally(t -> {
                    context.runOnContext(aVoid -> callback.onFailure(t));
                    return null;
                });
    }

}
