package com.englishtown.vertx.cassandra.zookeeper;

import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.englishtown.vertx.zookeeper.ZooKeeperClient;
import com.englishtown.vertx.zookeeper.promises.WhenConfiguratorHelper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.curator.utils.ZKPaths;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * ZooKeeper implementation of {@link com.englishtown.vertx.cassandra.CassandraConfigurator}
 */
public class ZooKeeperCassandraConfigurator extends EnvironmentCassandraConfigurator {

    private final WhenConfiguratorHelper helper;
    private final When when;
    private AsyncResult<Void> initResult;
    private final List<Handler<AsyncResult<Void>>> onReadyCallbacks = new ArrayList<>();
    protected String pathPrefix = "cassandra";

    @Inject
    public ZooKeeperCassandraConfigurator(ZooKeeperClient client, WhenConfiguratorHelper helper, When when, Vertx vertx, EnvVarDelegate envVarDelegate) {
        super(vertx, envVarDelegate);
        this.helper = helper;
        this.when = when;

        client.onReady(result -> {
            if (result.failed()) {
                runOnReadyCallbacks(result);
                return;
            }
            initZooKeeper();
        });
    }

    private void initZooKeeper() {

        List<Promise<Void>> promises = new ArrayList<>();

        if (DEFAULT_SEEDS.equals(seeds)) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "seeds")).then(
                    value -> {
                        JsonArray array = value.asJsonArray();
                        if (array != null) {
                            initSeeds(array);
                        }
                        return null;
                    }));
        }

        if (loadBalancingPolicy == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "policies/load_balancing")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initLoadBalancingPolicy(json);
                        }
                        return null;
                    }));
        }

        if (reconnectionPolicy == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "policies/reconnection")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initLoadBalancingPolicy(json);
                        }
                        return null;
                    }));
        }

        if (poolingOptions == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "pooling")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initPoolingOptions(json);
                        }
                        return null;
                    }));
        }

        if (socketOptions == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "socket")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initSocketOptions(json);
                        }
                        return null;
                    }));
        }

        if (queryOptions == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "query")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initQueryOptions(json);
                        }
                        return null;
                    }));
        }

        if (metricsOptions == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "metrics")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initMetricsOptions(json);
                        }
                        return null;
                    }));
        }

        if (authProvider == null) {
            promises.add(helper.getConfigElement(ZKPaths.makePath(getPathPrefix(), "auth")).then(
                    value -> {
                        JsonObject json = value.asJsonObject();
                        if (json != null) {
                            initAuthProvider(json);
                        }
                        return null;
                    }));
        }

        when.all(promises)
                .then(aVoid -> {
                    runOnReadyCallbacks(Future.succeededFuture(null));
                    return null;
                })
                .otherwise(t -> {
                    runOnReadyCallbacks(Future.failedFuture(t));
                    return null;
                });
    }

    private void runOnReadyCallbacks(AsyncResult<Void> result) {
        initResult = result;
        onReadyCallbacks.forEach(callback -> callback.handle(result));
        onReadyCallbacks.clear();
    }

    @Override
    public void onReady(Handler<AsyncResult<Void>> callback) {
        if (initResult != null) {
            callback.handle(initResult);
        } else {
            onReadyCallbacks.add(callback);
        }
    }

    protected String getPathPrefix() {
        return pathPrefix;
    }

}
