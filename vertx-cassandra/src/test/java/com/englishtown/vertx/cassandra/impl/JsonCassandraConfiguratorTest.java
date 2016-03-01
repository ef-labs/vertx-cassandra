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

        config.put(JsonCassandraConfigurator.CONFIG_SEEDS, new JsonArray()
                .add("127.0.0.1")
                .add("127.0.0.2")
                .add("127.0.0.3")
        );

        configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getSeeds());
        assertEquals(3, configurator.getSeeds().size());

    }

    @Test
    public void testGetPort_Null() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getPort());

    }

    @Test
    public void testGetPort() throws Exception {

        Integer port = 9043;
        config.put(JsonCassandraConfigurator.CONFIG_PORT, port);

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertEquals(port, configurator.getPort());

    }

    @Test
    public void testInitPolicies_LoadBalancing_No_Policies() throws Exception {
        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getLoadBalancingPolicy());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitPolicies_LoadBalancing_Missing_Name() throws Exception {
        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_LOAD_BALANCING, new JsonObject()));
        new JsonCassandraConfigurator(vertx);
    }

    @Test
    public void testInitPolicies_LoadBalancing_DCAwareRoundRobinPolicy() throws Exception {

        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_LOAD_BALANCING, new JsonObject()
                        .put("name", "DCAwareRoundRobinPolicy")
                        .put("local_dc", "US1")));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getLoadBalancingPolicy());
        assertThat(configurator.getLoadBalancingPolicy(), instanceOf(DCAwareRoundRobinPolicy.class));

    }

    @Test
    public void testInitPolicies_LoadBalancing_Custom() throws Exception {

        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_LOAD_BALANCING, new JsonObject()
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

    @Test(expected = IllegalArgumentException.class)
    public void testInitPolicies_Reconnection_Missing_Name() throws Exception {
        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_RECONNECTION, new JsonObject()));
        new JsonCassandraConfigurator(vertx);
    }

    @Test
    public void testInitPolicies_Reconnection_ConstantReconnectionPolicy() throws Exception {

        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_RECONNECTION, new JsonObject()
                        .put("name", "constant")
                        .put("delay", 1000)));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getReconnectionPolicy());
        assertThat(configurator.getReconnectionPolicy(), instanceOf(ConstantReconnectionPolicy.class));

    }

    @Test
    public void testInitPolicies_Reconnection_ExponentialReconnectionPolicy() throws Exception {

        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_RECONNECTION, new JsonObject()
                        .put("name", "exponential")
                        .put("base_delay", 1000)
                        .put("max_delay", 5000)));

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getReconnectionPolicy());
        assertThat(configurator.getReconnectionPolicy(), instanceOf(ExponentialReconnectionPolicy.class));

    }

    @Test
    public void testInitPolicies_Reconnection_Custom() throws Exception {

        config.put(JsonCassandraConfigurator.CONFIG_POLICIES, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_POLICIES_RECONNECTION, new JsonObject()
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

        config.put(JsonCassandraConfigurator.CONFIG_POOLING, new JsonObject()
                .put("core_connections_per_host_local", 1)
                .put("core_connections_per_host_remote", 2)
                .put("max_connections_per_host_local", 3)
                .put("max_connections_per_host_remote", 4)
                .put("max_simultaneous_requests_local", 5)
                .put("max_simultaneous_requests_remote", 6)
        );

        configurator = new JsonCassandraConfigurator(vertx);
        assertNotNull(configurator.getPoolingOptions());

        PoolingOptions options = configurator.getPoolingOptions();
        assertEquals(1, options.getCoreConnectionsPerHost(HostDistance.LOCAL));
        assertEquals(2, options.getCoreConnectionsPerHost(HostDistance.REMOTE));
        assertEquals(3, options.getMaxConnectionsPerHost(HostDistance.LOCAL));
        assertEquals(4, options.getMaxConnectionsPerHost(HostDistance.REMOTE));
        assertEquals(5, options.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL));
        assertEquals(6, options.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE));

    }

    @Test
    public void testGetSocketOptions() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.getSocketOptions());

        config.put(JsonCassandraConfigurator.CONFIG_SOCKET, new JsonObject()
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
        QueryOptions options;

        configurator = new JsonCassandraConfigurator(vertx);
        options = configurator.getQueryOptions();
        assertNotNull(options);
        assertEquals(QueryOptions.DEFAULT_CONSISTENCY_LEVEL, options.getConsistencyLevel());
        assertEquals(QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL, options.getSerialConsistencyLevel());
        assertEquals(QueryOptions.DEFAULT_FETCH_SIZE, options.getFetchSize());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, "");
        configurator = new JsonCassandraConfigurator(vertx);
        options = configurator.getQueryOptions();
        assertNotNull(options);
        assertEquals(QueryOptions.DEFAULT_CONSISTENCY_LEVEL, options.getConsistencyLevel());
        assertEquals(QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL, options.getSerialConsistencyLevel());
        assertEquals(QueryOptions.DEFAULT_FETCH_SIZE, options.getFetchSize());

        config.put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_ALL);
        configurator = new JsonCassandraConfigurator(vertx);
        options = configurator.getQueryOptions();
        assertNotNull(options);
        assertEquals(ConsistencyLevel.ALL, options.getConsistencyLevel());
        assertEquals(QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL, options.getSerialConsistencyLevel());
        assertEquals(QueryOptions.DEFAULT_FETCH_SIZE, options.getFetchSize());

        config.put(JsonCassandraConfigurator.CONFIG_QUERY, new JsonObject()
                .put(JsonCassandraConfigurator.CONFIG_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_LOCAL_ONE)
                .put(JsonCassandraConfigurator.CONFIG_SERIAL_CONSISTENCY_LEVEL, JsonCassandraConfigurator.CONSISTENCY_LOCAL_SERIAL)
                .put(JsonCassandraConfigurator.CONFIG_FETCH_SIZE, 11));

        configurator = new JsonCassandraConfigurator(vertx);
        options = configurator.getQueryOptions();
        assertNotNull(options);
        assertEquals(ConsistencyLevel.LOCAL_ONE, options.getConsistencyLevel());
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, options.getSerialConsistencyLevel());
        assertEquals(11, options.getFetchSize());

    }

    @Test
    public void testGetConsistencyLevel() throws Exception {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(config);
        ConsistencyLevel consistency;

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_ANY);
        assertEquals(ConsistencyLevel.ANY, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_ONE);
        assertEquals(ConsistencyLevel.ONE, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_TWO);
        assertEquals(ConsistencyLevel.TWO, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_THREE);
        assertEquals(ConsistencyLevel.THREE, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_QUORUM);
        assertEquals(ConsistencyLevel.QUORUM, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_ALL);
        assertEquals(ConsistencyLevel.ALL, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_LOCAL_QUORUM);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_EACH_QUORUM);
        assertEquals(ConsistencyLevel.EACH_QUORUM, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_SERIAL);
        assertEquals(ConsistencyLevel.SERIAL, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_LOCAL_SERIAL);
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, consistency);

        consistency = configurator.getConsistency(JsonCassandraConfigurator.CONSISTENCY_LOCAL_ONE);
        assertEquals(ConsistencyLevel.LOCAL_ONE, consistency);

        try {
            configurator.getConsistency("invalid consistency");
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

        config.put(JsonCassandraConfigurator.CONFIG_METRICS, metrics);
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
        config.put(JsonCassandraConfigurator.CONFIG_AUTH, auth);

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
