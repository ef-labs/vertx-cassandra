package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import java.util.Collection;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator}
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonCassandraConfiguratorTest {

    JsonObject config = new JsonObject();
    @Mock
    Container container;

    public static class TestLoadBalancingPolicy implements LoadBalancingPolicy {
        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
        }

        @Override
        public HostDistance distance(Host host) {
            return null;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return null;
        }

        @Override
        public void onAdd(Host host) {
        }

        @Override
        public void onUp(Host host) {
        }

        @Override
        public void onDown(Host host) {
        }

        @Override
        public void onRemove(Host host) {
        }
    }

    @Before
    public void setUp() throws Exception {
        JsonObject baseConfig = new JsonObject().putObject("cassandra", config);
        when(container.config()).thenReturn(baseConfig);
    }

    @Test
    public void testGetSeeds() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(container);
        assertNotNull(configurator.getSeeds());
        assertFalse(configurator.getSeeds().isEmpty());
        assertEquals("127.0.0.1", configurator.getSeeds().get(0));

        config.putArray("seeds", new JsonArray()
                .addString("127.0.0.1")
                .addString("127.0.0.2")
                .addString("127.0.0.3")
        );

        configurator = new JsonCassandraConfigurator(container);
        assertNotNull(configurator.getSeeds());
        assertEquals(3, configurator.getSeeds().size());

    }

    @Test
    public void testInitPolicies_LoadBalancing_No_Policies() throws Exception {
        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(container);
        assertNull(configurator.getLoadBalancingPolicy());
    }

    @Test
    public void testInitPolicies_LoadBalancing_Missing_Name() throws Exception {
        config.putObject("policies", new JsonObject().putObject("load_balancing", new JsonObject()));
        try {
            new JsonCassandraConfigurator(container);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testInitPolicies_LoadBalancing_DCAwareRoundRobinPolicy() throws Exception {

        config.putObject("policies", new JsonObject()
                .putObject("load_balancing", new JsonObject()
                        .putString("name", "DCAwareRoundRobinPolicy")
                        .putString("local_dc", "US1")));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(container);
        assertNotNull(configurator.getLoadBalancingPolicy());
        assertThat(configurator.getLoadBalancingPolicy(), instanceOf(DCAwareRoundRobinPolicy.class));

    }

    @Test
    public void testInitPolicies_LoadBalancing_Custom() throws Exception {

        config.putObject("policies", new JsonObject()
                .putObject("load_balancing", new JsonObject()
                        .putString("name", "com.englishtown.vertx.cassandra.impl.JsonCassandraConfiguratorTest$TestLoadBalancingPolicy")
                ));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(container);
        assertNotNull(configurator.getLoadBalancingPolicy());
        assertThat(configurator.getLoadBalancingPolicy(), instanceOf(TestLoadBalancingPolicy.class));

    }

    @Test
    public void testGetPoolingOptions() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(container);
        assertNull(configurator.getPoolingOptions());

        config.putObject("pooling", new JsonObject()
                .putNumber("core_connections_per_host_local", 1)
                .putNumber("core_connections_per_host_remote", 2)
                .putNumber("max_connections_per_host_local", 3)
                .putNumber("max_connections_per_host_remote", 4)
                .putNumber("min_simultaneous_requests_local", 5)
                .putNumber("min_simultaneous_requests_remote", 6)
                .putNumber("max_simultaneous_requests_local", 7)
                .putNumber("max_simultaneous_requests_remote", 8)
        );

        configurator = new JsonCassandraConfigurator(container);
        assertNotNull(configurator.getPoolingOptions());

        PoolingOptions options = configurator.getPoolingOptions();
        assertEquals(1, options.getCoreConnectionsPerHost(HostDistance.LOCAL));
        assertEquals(2, options.getCoreConnectionsPerHost(HostDistance.REMOTE));
        assertEquals(3, options.getMaxConnectionsPerHost(HostDistance.LOCAL));
        assertEquals(4, options.getMaxConnectionsPerHost(HostDistance.REMOTE));
        assertEquals(5, options.getMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL));
        assertEquals(6, options.getMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE));
        assertEquals(7, options.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL));
        assertEquals(8, options.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE));

    }

    @Test
    public void testGetConsistency() throws Exception {

        JsonCassandraConfigurator configurator;

        configurator = new JsonCassandraConfigurator(container);
        ConsistencyLevel consistency = configurator.getConsistency();
        assertNull(consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, "");
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertNull(consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ALL);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.ALL, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ANY);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.ANY, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_EACH_QUORUM);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.EACH_QUORUM, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_LOCAL_QUORUM);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ONE);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.ONE, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_QUORUM);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.QUORUM, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_THREE);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.THREE, consistency);

        config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_TWO);
        configurator = new JsonCassandraConfigurator(container);

        consistency = configurator.getConsistency();
        assertEquals(ConsistencyLevel.TWO, consistency);

        try {
            config.putString(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, "invalid consistency");
            new JsonCassandraConfigurator(container);

            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }

    }
}
