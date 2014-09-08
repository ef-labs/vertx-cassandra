package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Container;

import javax.inject.Inject;

/**
 *
 */
public class EnvironmentCassandraConfigurator extends JsonCassandraConfigurator {

    // The environment variable that contains the pipe delimited list of seeds
    public static final String ENV_VAR_SEEDS = "CASSANDRA_SEEDS";
    public static final String ENV_VAR_LOCAL_DC = "CASSANDRA_LOCAL_DC";
    public static final String ENV_VAR_USERNAME = "CASSANDRA_USERNAME";
    public static final String ENV_VAR_PASSWORD = "CASSANDRA_PASSWORD";

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
            logger.debug("No environment configuration for seeds found, so falling back to JSON configuration.");
            super.initSeeds(config);
        } else {
            logger.debug("Using environment configuration of " + envVarSeeds);
            String[] seedsArray = envVarSeeds.split("\\|");
            seeds = ImmutableList.copyOf(seedsArray);
        }
    }

    @Override
    protected void initPolicies(JsonObject config) {

        String envVarLocalDC = container.env().get(ENV_VAR_LOCAL_DC);

        // If the environment variable is not defined, then we just fall back on the JSON config
        if (Strings.isNullOrEmpty(envVarLocalDC)) {
            logger.debug("No environment configuration found for local DC, so falling back on JSON configuration.");
            super.initPolicies(config);
        } else {
            logger.debug("Using environment config for Local DC of " + envVarLocalDC);

            // We take the current config, remove any local dc configuration and replace it with our environment var version
            JsonObject policies = config.getObject("policies");
            if (policies == null) {
                policies = new JsonObject();
                config.putObject("policies", policies);
            }

            JsonObject loadBalancing = new JsonObject();
            loadBalancing.putString("name", "DCAwareRoundRobinPolicy").putString("local_dc", envVarLocalDC);

            policies.putObject("load_balancing", loadBalancing);

            super.initPolicies(config);
        }
    }

    @Override
    protected void initAuthProvider(JsonObject config) {

        String username = container.env().get(ENV_VAR_USERNAME);
        String password = container.env().get(ENV_VAR_PASSWORD);

        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            authProvider = new PlainTextAuthProvider(username, password);
        } else {
            super.initAuthProvider(config);
        }

    }
}
