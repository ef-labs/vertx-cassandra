package com.englishtown.vertx.cassandra.impl;

import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.zookeeper.ZooKeeperClient;
import com.englishtown.vertx.zookeeper.promises.WhenConfiguratorHelper;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * ZooKeeper implementation of {@link com.englishtown.vertx.cassandra.CassandraConfigurator}
 */
public class ZooKeeperCassandraConfigurator extends EnvironmentCassandraConfigurator {

    private final ZooKeeperClient client;
    private final WhenConfiguratorHelper helper;
    private final When when;
    private AsyncResult<Void> initResult;
    private final List<Handler<AsyncResult<Void>>> onReadyCallbacks = new ArrayList<>();

    @Inject
    public ZooKeeperCassandraConfigurator(ZooKeeperClient client, WhenConfiguratorHelper helper, Container container, When when) {
        super(container);
        this.client = client;
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

        if (!(seeds.size() == 1 && "127.0.0.1".equals(seeds.<String>get(0)))) {
            promises.add(helper.getConfigElement("/cassandra/seeds").then(
                    value -> {
                        JsonArray array = value.asJsonArray();
                        if (array != null) {
                            initSeeds(array);
                        }
                        return null;
                    }));
        }

        promises.add(helper.getConfigElement("/cassandra/policies/load_balancing").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initLoadBalancingPolicy(json);
                    }
                    return null;
                }));

        promises.add(helper.getConfigElement("/cassandra/policies/reconnection").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initLoadBalancingPolicy(json);
                    }
                    return null;
                }));

        promises.add(helper.getConfigElement("/cassandra/pooling").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initPoolingOptions(json);
                    }
                    return null;
                }));

        promises.add(helper.getConfigElement("/cassandra/socket").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initSocketOptions(json);
                    }
                    return null;
                }));

        promises.add(helper.getConfigElement("/cassandra/query").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initQueryOptions(json);
                    }
                    return null;
                }));

        promises.add(helper.getConfigElement("/cassandra/metrics").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initMetricsOptions(json);
                    }
                    return null;
                }));

        promises.add(helper.getConfigElement("/cassandra/auth").then(
                value -> {
                    JsonObject json = value.asJsonObject();
                    if (json != null) {
                        initAuthProvider(json);
                    }
                    return null;
                }));

        when.all(promises)
                .then(aVoid -> {
                    runOnReadyCallbacks(new DefaultFutureResult<>((Void) null));
                    return null;
                })
                .otherwise(t -> {
                    runOnReadyCallbacks(new DefaultFutureResult<>(t));
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
            callback.handle(new DefaultFutureResult<>((Void) null));
        } else {
            onReadyCallbacks.add(callback);
        }
    }

}
