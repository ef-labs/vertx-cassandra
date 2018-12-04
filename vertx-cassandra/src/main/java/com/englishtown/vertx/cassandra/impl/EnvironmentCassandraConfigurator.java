package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

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
    public static final String ENV_KEYSPACE = "KEYSPACE";

    public static final Logger logger = LoggerFactory.getLogger(EnvironmentCassandraConfigurator.class);
    private final EnvVarDelegate envVarDelegate;

    @Inject
    public EnvironmentCassandraConfigurator(Vertx vertx, EnvVarDelegate envVarDelegate) {
        super(vertx);
        this.envVarDelegate = envVarDelegate;
        init();
    }

    public EnvironmentCassandraConfigurator(JsonObject config, EnvVarDelegate envVarDelegate) {
        super(config);
        this.envVarDelegate = envVarDelegate;
        init();
    }

    private void init() {
        initSeeds();
        initLoadBalancingPolicy();
        initAuthProvider();
        initKeyspace();
    }

    private void initSeeds() {
        String envVarSeeds = envVarDelegate.get(ENV_VAR_SEEDS);

        if (!Strings.isNullOrEmpty(envVarSeeds)) {
            logger.debug("Using environment configuration of " + envVarSeeds);
            String[] seedsArray = envVarSeeds.split("\\|");
            this.seeds = ImmutableList.copyOf(seedsArray);
        }
    }

    private void initLoadBalancingPolicy() {
        String localDC = envVarDelegate.get(ENV_VAR_LOCAL_DC);

        if (!Strings.isNullOrEmpty(localDC)) {
            logger.debug("Using environment config for Local DC of " + localDC);
            loadBalancingPolicy = DCAwareRoundRobinPolicy.builder()
                    .withLocalDc(localDC)
                    .build();
        } else {
            logger.debug("No environment configuration found for local DC");
        }
    }

    private void initAuthProvider() {
        String username = envVarDelegate.get(ENV_VAR_USERNAME);
        String password = envVarDelegate.get(ENV_VAR_PASSWORD);

        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            authProvider = new PlainTextAuthProvider(username, password);
        }
    }

    public interface EnvVarDelegate {
        String get(String name);
    }

    public static class DefaultEnvVarDelegate implements EnvVarDelegate {

        @Override
        public String get(String name) {
            return System.getenv(name);
        }
    }

    private void initKeyspace(){
        String keyspace = envVarDelegate.get(ENV_KEYSPACE);
        if (!Strings.isNullOrEmpty(keyspace)){
            this.keyspace = keyspace;
        }

    }
}
