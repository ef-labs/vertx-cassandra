package com.englishtown.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.google.common.base.Strings;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Json configuration based implementation of {@link com.englishtown.vertx.cassandra.CassandraConfigurator}
 */
public class JsonCassandraConfigurator implements CassandraConfigurator {

    private final JsonObject config;
    protected CqlSessionBuilder sessionBuilder;


    public static final String CONFIG_CASSANDRA = "cassandra";
    public static final String CONFIG_SEEDS = "seeds";
    public static final String CONFIG_AUTH = "auth";
    public static final String CONFIG_LOCAL_DC = "local_dc";

    public static final int DEFAULT_PORT = 9042;

    @Inject
    public JsonCassandraConfigurator(Vertx vertx) {
        this(vertx.getOrCreateContext().config().getJsonObject(CONFIG_CASSANDRA, new JsonObject()));
    }

    public JsonCassandraConfigurator(JsonObject config) {
        this.config = config;
        sessionBuilder = CqlSession.builder();
        init();
    }

    @Override
    public void onReady(Handler<AsyncResult<CqlSessionBuilder>> callback) {
        callback.handle(Future.succeededFuture(sessionBuilder));
    }

    protected void init() {

        Collection<InetSocketAddress> seeds = initSeeds();
        if (seeds != null) {
            sessionBuilder.addContactPoints(seeds);
        }

        String localDatacenter = initLocalDatacenter();
        if (!Strings.isNullOrEmpty(localDatacenter)) {
            sessionBuilder.withLocalDatacenter(localDatacenter);
        }

        AuthProvider authProvider = initAuthProvider();
        if (authProvider != null) {
            sessionBuilder.withAuthProvider(authProvider);
        }
    }

    protected Collection<InetSocketAddress> initSeeds() {

        JsonArray seeds = config.getJsonArray(CONFIG_SEEDS);
        // Get array of IPs, default to localhost
        if (seeds == null || seeds.size() == 0) {
            return null;
        }

        List<InetSocketAddress> results = new ArrayList<>();

        for (int i = 0; i < seeds.size(); i++) {
            results.add(parseSeed(seeds.getString(i)));
        }

        return results;
    }

    protected InetSocketAddress parseSeed(String seed) {
        String[] split = seed.split(":");
        String hostName = split[0];
        int port = split.length > 1 ? Integer.parseInt(split[1]) : DEFAULT_PORT;
        return new InetSocketAddress(hostName, port);
    }

    protected String initLocalDatacenter() {
        return config.getString(CONFIG_LOCAL_DC);
    }

    protected AuthProvider initAuthProvider() {

        JsonObject auth = config.getJsonObject(CONFIG_AUTH);
        if (auth == null) {
            return null;
        }

        String username = auth.getString("username");
        String password = auth.getString("password");

        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalArgumentException("A username field must be provided on an auth field.");
        }
        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalArgumentException("A password field must be provided on an auth field.");
        }

        return new ProgrammaticPlainTextAuthProvider(username, password);

    }

}
