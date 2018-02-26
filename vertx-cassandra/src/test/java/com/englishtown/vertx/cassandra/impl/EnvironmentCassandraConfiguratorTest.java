package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator.CONFIG_CASSANDRA;
import static com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator.CONFIG_SEEDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator}
 */
@RunWith(MockitoJUnitRunner.class)
public class EnvironmentCassandraConfiguratorTest {

    JsonObject config = new JsonObject();
    JsonObject cassConfig;

    @Mock
    Vertx vertx;
    @Mock
    Context context;
    @Mock
    EnvironmentCassandraConfigurator.EnvVarDelegate envVarDelegate;

    @Before
    public void setUp() {
        when(context.config()).thenReturn(config);
        when(vertx.getOrCreateContext()).thenReturn(context);

        config.put(CONFIG_CASSANDRA, cassConfig = new JsonObject());
    }


    @Test
    public void testInitSeeds_Defaults() throws Exception {

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(vertx, envVarDelegate);
        List<String> seeds = configurator.getSeeds();

        assertNotNull(seeds);
        assertEquals(1, seeds.size());
        assertEquals("127.0.0.1", seeds.get(0));
    }

    @Test
    public void testInitSeeds() throws Exception {

        cassConfig.put(CONFIG_SEEDS, new JsonArray().add("127.0.0.5"));
        when(envVarDelegate.get(eq(EnvironmentCassandraConfigurator.ENV_VAR_SEEDS))).thenReturn("127.0.0.2|127.0.0.3|127.0.0.4");

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(vertx, envVarDelegate);
        List<String> seeds = configurator.getSeeds();

        assertNotNull(seeds);
        assertEquals(3, seeds.size());
        assertEquals("127.0.0.2", seeds.get(0));
        assertEquals("127.0.0.3", seeds.get(1));
        assertEquals("127.0.0.4", seeds.get(2));
    }

    @Test
    public void testInitPolicies() throws Exception {

        when(envVarDelegate.get(eq(EnvironmentCassandraConfigurator.ENV_VAR_LOCAL_DC))).thenReturn("LOCAL1");

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, envVarDelegate);
        LoadBalancingPolicy loadBalancingPolicy = configurator.getLoadBalancingPolicy();

        assertThat(loadBalancingPolicy, instanceOf(DCAwareRoundRobinPolicy.class));
    }

    @Test
    public void testInitAuthProvider() throws Exception {

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, envVarDelegate);
        AuthProvider authProvider = configurator.getAuthProvider();

        assertNull(authProvider);

        String username = "test";
        String password = "test_password";
        when(envVarDelegate.get(eq(EnvironmentCassandraConfigurator.ENV_VAR_USERNAME))).thenReturn(username);
        when(envVarDelegate.get(eq(EnvironmentCassandraConfigurator.ENV_VAR_PASSWORD))).thenReturn(password);

        configurator = new EnvironmentCassandraConfigurator(config, envVarDelegate);
        authProvider = configurator.getAuthProvider();

        assertThat(authProvider, instanceOf(PlainTextAuthProvider.class));

    }
}
