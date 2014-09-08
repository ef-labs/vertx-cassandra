package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator}
 */
@RunWith(MockitoJUnitRunner.class)
public class EnvironmentCassandraConfiguratorTest {

    JsonObject config = new JsonObject();
    Map<String, String> env = new HashMap<>();

    @Mock
    Container container;

    @Before
    public void setUp() {
        when(container.config()).thenReturn(config);
        when(container.env()).thenReturn(env);
    }


    @Test
    public void testInitSeeds_Defaults() throws Exception {

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(container);
        List<String> seeds = configurator.getSeeds();

        assertNotNull(seeds);
        assertEquals(1, seeds.size());
        assertEquals("127.0.0.1", seeds.get(0));
    }

    @Test
    public void testInitSeeds() throws Exception {

        env.put(EnvironmentCassandraConfigurator.ENV_VAR_SEEDS, "127.0.0.2|127.0.0.3|127.0.0.4");

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, container);
        List<String> seeds = configurator.getSeeds();

        assertNotNull(seeds);
        assertEquals(3, seeds.size());
        assertEquals("127.0.0.2", seeds.get(0));
        assertEquals("127.0.0.3", seeds.get(1));
        assertEquals("127.0.0.4", seeds.get(2));
    }

    @Test
    public void testInitPolicies() throws Exception {

        env.put(EnvironmentCassandraConfigurator.ENV_VAR_LOCAL_DC, "LOCAL1");

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, container);
        LoadBalancingPolicy loadBalancingPolicy = configurator.getLoadBalancingPolicy();

        assertTrue(loadBalancingPolicy instanceof DCAwareRoundRobinPolicy);
    }

    @Test
    public void testInitAuthProvider() throws Exception {

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, container);
        AuthProvider authProvider = configurator.getAuthProvider();

        assertNull(authProvider);

        String username = "test";
        String password = "test_password";
        env.put(EnvironmentCassandraConfigurator.ENV_VAR_USERNAME, username);
        env.put(EnvironmentCassandraConfigurator.ENV_VAR_PASSWORD, password);

        configurator = new EnvironmentCassandraConfigurator(config, container);
        authProvider = configurator.getAuthProvider();

        assertThat(authProvider, instanceOf(PlainTextAuthProvider.class));

    }
}
