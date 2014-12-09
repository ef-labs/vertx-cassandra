package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.vertx.java.core.json.JsonArray;
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
    protected void initSeeds(JsonArray seeds) {

        // Recall super
        super.initSeeds(seeds);

        // If default, try env vars
        if (DEFAULT_SEEDS.equals(this.seeds)) {
            String envVarSeeds = container.env().get(ENV_VAR_SEEDS);

            if (!Strings.isNullOrEmpty(envVarSeeds)) {
                logger.debug("Using environment configuration of " + envVarSeeds);
                String[] seedsArray = envVarSeeds.split("\\|");
                this.seeds = ImmutableList.copyOf(seedsArray);
            }
        }
    }

    @Override
    protected void initPolicies(JsonObject policyConfig) {
        super.initPolicies(policyConfig == null ? new JsonObject() : policyConfig);
    }

    @Override
    protected void initLoadBalancingPolicy(JsonObject loadBalancing) {

        // Recall super
        super.initLoadBalancingPolicy(loadBalancing);

        // If LB policy not set, try env vars
        if (loadBalancingPolicy == null) {
            String localDC = container.env().get(ENV_VAR_LOCAL_DC);

            if (!Strings.isNullOrEmpty(localDC)) {
                logger.debug("Using environment config for Local DC of " + localDC);
                loadBalancingPolicy = new DCAwareRoundRobinPolicy(localDC);
            } else {
                logger.debug("No environment configuration found for local DC");
            }
        }
    }

    @Override
    protected void initAuthProvider(JsonObject auth) {

        // Recall super
        super.initAuthProvider(auth);

        // If auth provider not set, try env vars
        if (authProvider == null) {
            String username = container.env().get(ENV_VAR_USERNAME);
            String password = container.env().get(ENV_VAR_PASSWORD);

            if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
                authProvider = new PlainTextAuthProvider(username, password);
            }
        }

    }
}
