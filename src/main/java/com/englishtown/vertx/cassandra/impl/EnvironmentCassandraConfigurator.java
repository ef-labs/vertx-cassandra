package com.englishtown.vertx.cassandra.impl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import javax.inject.Inject;

/**
 *
 */
public class EnvironmentCassandraConfigurator extends JsonCassandraConfigurator {

    // The environment variable that contains the pipe delimited list of seeds
    public static final String ENV_VAR_SEEDS = "CASSANDRA_SEEDS";
    public static final Logger logger = LoggerFactory.getLogger(EnvironmentCassandraConfigurator.class);

    @Inject
    public EnvironmentCassandraConfigurator(Container container) {
        this(container.config().getObject("cassandra", new JsonObject()), container);
    }

    public EnvironmentCassandraConfigurator(JsonObject config, Container container) {
        super(config, container);
    }

    @Override
    protected void initSeeds(JsonObject config) {

        String envVarSeeds = container.env().get(ENV_VAR_SEEDS);

        // If no environment variable is set up, we fall back on the JSON config
        if (Strings.isNullOrEmpty(envVarSeeds)) {
            logger.debug("No environment configuration found, so falling back to JSON configuration");
            super.initSeeds(config);
        } else {
            logger.debug("Using environment configuration of ", envVarSeeds);
            String[] seedsArray = envVarSeeds.split("\\|");
            seeds = ImmutableList.copyOf(seedsArray);
        }
    }
}
