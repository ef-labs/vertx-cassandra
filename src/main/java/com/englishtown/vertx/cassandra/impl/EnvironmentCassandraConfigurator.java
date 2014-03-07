package com.englishtown.vertx.cassandra.impl;

import com.google.common.base.Strings;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class EnvironmentCassandraConfigurator extends JsonCassandraConfigurator {

    public static final String CONFIG_ENV_SEEDS = "env_seeds";


    @Inject
    public EnvironmentCassandraConfigurator(Container container) {
        this(container.config().getObject("cassandra", new JsonObject()));
    }

    public EnvironmentCassandraConfigurator(JsonObject config) {
        super(config);
    }

    @Override
    protected void initSeeds(JsonObject config) {

        // First get the environment variables to check
        JsonArray envSeeds = config.getArray(CONFIG_ENV_SEEDS);

        // If this is missing or empty, then we call the superclass method
        if (envSeeds == null || envSeeds.size() == 0) {
            super.initSeeds(config);
        } else {
            seeds = getSeedsFromEnv(envSeeds);
            if (seeds == null || seeds.isEmpty()) super.initSeeds(config);
        }
    }

    private List<String> getSeedsFromEnv(JsonArray envSeeds) {
        List<String> ips = new ArrayList<>(envSeeds.size());

        for (int i = 0; i < envSeeds.size(); i++) {
            String envValue = System.getenv(envSeeds.<String>get(i));
            if (!Strings.isNullOrEmpty(envValue)) {
                ips.add(envValue);      // TODO: Should really perform a check to confirm that it's a valid IP Address
            }
        }

        return ips;
    }
}
