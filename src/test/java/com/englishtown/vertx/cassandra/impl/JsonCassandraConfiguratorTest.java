package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
    Vertx vertx;
    @Mock
    Context context;

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
        public void onSuspected(Host host) {
        }

        @Override
        public void onDown(Host host) {
        }

        @Override
        public void onRemove(Host host) {
        }
    }

    public static class TestReconnectionPolicy implements ReconnectionPolicy {
        @Override
        public ReconnectionSchedule newSchedule() {
            return null;
        }
    }

    @Before
    public void setUp() throws Exception {
        JsonObject baseConfig = new JsonObject().put("cassandra", config);
        when(context.config()).thenReturn(baseConfig);
        when(vertx.getOrCreateContext()).thenReturn(context);
    }

    @Test
    public void testGetSeeds() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getSeeds());
        assertFalse(configurator.getSeeds().isEmpty());
        assertEquals("127.0.0.1", configurator.getSeeds().get(0));

        config.put("seeds", new JsonArray()
                        .add("127.0.0.1")
                        .add("127.0.0.2")
                        .add("127.0.0.3")
        );

        configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getSeeds());
        assertEquals(3, configurator.getSeeds().size());

    }

    @Test
    public void testInitPolicies_LoadBalancing_No_Policies() throws Exception {
        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getLoadBalancingPolicy());
    }

    @Test
    public void testInitPolicies_LoadBalancing_Missing_Name() throws Exception {
        config.put("policies", new JsonObject().put("load_balancing", new JsonObject()));
        try {
            new JsonCassandraConfigurator(vertx);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testInitPolicies_LoadBalancing_DCAwareRoundRobinPolicy() throws Exception {

        config.put("policies", new JsonObject()
                .put("load_balancing", new JsonObject()
                        .put("name", "DCAwareRoundRobinPolicy")
                        .put("local_dc", "US1")));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getLoadBalancingPolicy());
        assertThat(configurator.getLoadBalancingPolicy(), instanceOf(DCAwareRoundRobinPolicy.class));

    }

    @Test
    public void testInitPolicies_LoadBalancing_Custom() throws Exception {

        config.put("policies", new JsonObject()
                .put("load_balancing", new JsonObject()
                                .put("name", TestLoadBalancingPolicy.class.getName())
                ));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getLoadBalancingPolicy());
        assertThat(configurator.getLoadBalancingPolicy(), instanceOf(TestLoadBalancingPolicy.class));

    }

    @Test
    public void testInitPolicies_Reconnection_No_Policies() throws Exception {
        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getReconnectionPolicy());
    }

    @Test
    public void testInitPolicies_Reconnection_Missing_Name() throws Exception {
        config.put("policies", new JsonObject().put("reconnection", new JsonObject()));
        try {
            new JsonCassandraConfigurator(vertx);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testInitPolicies_Reconnection_ConstantReconnectionPolicy() throws Exception {

        config.put("policies", new JsonObject()
                .put("reconnection", new JsonObject()
                        .put("name", "constant")
                        .put("delay", 1000)));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getReconnectionPolicy());
        assertThat(configurator.getReconnectionPolicy(), instanceOf(ConstantReconnectionPolicy.class));

    }

    @Test
    public void testInitPolicies_Reconnection_ExponentialReconnectionPolicy() throws Exception {

        config.put("policies", new JsonObject()
                .put("reconnection", new JsonObject()
                        .put("name", "exponential")
                        .put("base_delay", 1000)
                        .put("max_delay", 5000)));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getReconnectionPolicy());
        assertThat(configurator.getReconnectionPolicy(), instanceOf(ExponentialReconnectionPolicy.class));

    }

    @Test
    public void testInitPolicies_Reconnection_Custom() throws Exception {

        config.put("policies", new JsonObject()
                .put("reconnection", new JsonObject()
                                .put("name", TestReconnectionPolicy.class.getName())
                ));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getReconnectionPolicy());
        assertThat(configurator.getReconnectionPolicy(), instanceOf(TestReconnectionPolicy.class));

    }

    @Test
    public void testGetPoolingOptions() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getPoolingOptions());

        config.put("pooling", new JsonObject()
                        .put("core_connections_per_host_local", 1)
                        .put("core_connections_per_host_remote", 2)
                        .put("max_connections_per_host_local", 3)
                        .put("max_connections_per_host_remote", 4)
                        .put("min_simultaneous_requests_local", 5)
                        .put("min_simultaneous_requests_remote", 6)
                        .put("max_simultaneous_requests_local", 7)
                        .put("max_simultaneous_requests_remote", 8)
        );

        configurator = new JsonCassandraConfigurator(vertx);
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
    public void testGetSocketOptions() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getSocketOptions());

        config.put("socket", new JsonObject()
                        .put("connect_timeout_millis", 32000)
                        .put("read_timeout_millis", 33000)
                        .put("keep_alive", true)
                        .put("reuse_address", true)
                        .put("receive_buffer_size", 1024)
                        .put("send_buffer_size", 2048)
                        .put("so_linger", 10)
                        .put("tcp_no_delay", false)
        );

        configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getSocketOptions());

        SocketOptions options = configurator.getSocketOptions();
        assertEquals(32000, options.getConnectTimeoutMillis());
        assertEquals(33000, options.getReadTimeoutMillis());
        assertEquals(true, options.getKeepAlive());
        assertEquals(true, options.getReuseAddress());
        assertEquals(1024, options.getReceiveBufferSize().intValue());
        assertEquals(2048, options.getSendBufferSize().intValue());
        assertEquals(10, options.getSoLinger().intValue());
        assertEquals(false, options.getTcpNoDelay());

    }

    @Test
    public void testGetQueryOptions() throws Exception {

        JsonCassandraConfigurator configurator;

        configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getQueryOptions());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, "");
        configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getQueryOptions());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ALL);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.ALL, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ANY);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.ANY, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_EACH_QUORUM);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.EACH_QUORUM, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_LOCAL_ONE);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.LOCAL_ONE, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_LOCAL_QUORUM);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ONE);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.ONE, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_QUORUM);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.QUORUM, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_THREE);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.THREE, configurator.getQueryOptions().getConsistencyLevel());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_TWO);
        configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(ConsistencyLevel.TWO, configurator.getQueryOptions().getConsistencyLevel());

        try {
            config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, "invalid consistency");
            new JsonCassandraConfigurator(vertx);

            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }

    }

    @Test
    public void testGetMetricsOptions() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getMetricsOptions());

        JsonObject metrics = new JsonObject();

        config.put("metrics", metrics);
        metrics.put("jmx_enabled", true);
        configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getMetricsOptions());
        assertTrue(configurator.getMetricsOptions().isJMXReportingEnabled());

        metrics.put("jmx_enabled", false);
        configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getMetricsOptions());
        assertFalse(configurator.getMetricsOptions().isJMXReportingEnabled());

    }

    @Test
    public void testInitAuthProvider() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(config);
        AuthProvider authProvider = configurator.getAuthProvider();

        assertNull(authProvider);

        String username = "test";
        String password = "test_password";
        JsonObject auth = new JsonObject()
                .put("username", username)
                .put("password", password);

        config.put("auth", auth);

        configurator = new JsonCassandraConfigurator(config);
        authProvider = configurator.getAuthProvider();

        assertThat(authProvider, instanceOf(PlainTextAuthProvider.class));

    }

    @Test
    public void testInitAuthProvider_fail() throws Exception {

        String username = "test";
        String password = "test_password";

        JsonObject auth = new JsonObject();
        config.put("auth", auth);

        try {
            new JsonCassandraConfigurator(config);
            fail("Expected error for missing username/password");
        } catch (IllegalArgumentException e) {
            // expected
        }

        auth.put("username", username);

        try {
            new JsonCassandraConfigurator(config);
            fail("Expected error for missing username/password");
        } catch (IllegalArgumentException e) {
            // expected
        }

        auth.remove("username");
        auth.put("password", password);

        try {
            new JsonCassandraConfigurator(config);
            fail("Expected error for missing username/password");
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

}
