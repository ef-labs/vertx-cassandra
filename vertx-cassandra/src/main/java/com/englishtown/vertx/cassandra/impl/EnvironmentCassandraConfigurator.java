package com.englishtown.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.google.common.base.Strings;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

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
    public EnvironmentCassandraConfigurator(Vertx vertx) {
        super(vertx);
    }

    public EnvironmentCassandraConfigurator(JsonObject config) {
        super(config);
    }


    @Override
    protected Collection<InetSocketAddress> initSeeds() {
        String envVarSeeds = System.getenv(ENV_VAR_SEEDS);

        if (!Strings.isNullOrEmpty(envVarSeeds)) {
            logger.debug("Using environment configuration of " + envVarSeeds);
            String[] seedsArray = envVarSeeds.split("\\|");
            return Arrays.stream(seedsArray).map(this::parseSeed).collect(Collectors.toList());
        }

        return super.initSeeds();
    }

    @Override
    protected String initLocalDatacenter() {
        String localDataCenter = System.getenv(ENV_VAR_LOCAL_DC);

        if (!Strings.isNullOrEmpty(localDataCenter)) {
            return localDataCenter;
        }

        return super.initLocalDatacenter();
    }

    @Override
    protected AuthProvider initAuthProvider() {
        String username = System.getenv(ENV_VAR_USERNAME);
        String password = System.getenv(ENV_VAR_PASSWORD);

        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            return new ProgrammaticPlainTextAuthProvider(username, password);
        }

        return super.initAuthProvider();
    }

}
