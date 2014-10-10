package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.google.common.base.Strings;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Json configuration based implementation of {@link com.englishtown.vertx.cassandra.CassandraConfigurator}
 */
public class JsonCassandraConfigurator implements CassandraConfigurator {

    protected final Container container;

    protected List<String> seeds;
    protected LoadBalancingPolicy loadBalancingPolicy;
    protected ReconnectionPolicy reconnectionPolicy;
    protected PoolingOptions poolingOptions;
    protected SocketOptions socketOptions;
    protected QueryOptions queryOptions;
    protected MetricsOptions metricsOptions;
    protected AuthProvider authProvider;

    public static final String CONFIG_SEEDS = "seeds";
    public static final String CONFIG_POLICIES = "policies";
    public static final String CONFIG_POOLING = "pooling";
    public static final String CONFIG_SOCKET = "socket";
    public static final String CONFIG_METRICS = "metrics";
    public static final String CONFIG_AUTH = "auth";
    public static final String CONFIG_CONSISTENCY_LEVEL = "consistency_level";

    public static final String CONSISTENCY_ANY = "ANY";
    public static final String CONSISTENCY_ONE = "ONE";
    public static final String CONSISTENCY_TWO = "TWO";
    public static final String CONSISTENCY_THREE = "THREE";
    public static final String CONSISTENCY_QUORUM = "QUORUM";
    public static final String CONSISTENCY_ALL = "ALL";
    public static final String CONSISTENCY_LOCAL_ONE = "LOCAL_ONE";
    public static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    public static final String CONSISTENCY_EACH_QUORUM = "EACH_QUORUM";

    @Inject
    public JsonCassandraConfigurator(Container container) {
        this(container.config().getObject("cassandra", new JsonObject()), container);
    }

    public JsonCassandraConfigurator(JsonObject config, Container container) {
        this.container = container;
        init(config);
    }

    @Override
    public List<String> getSeeds() {
        return seeds;
    }

    @Override
    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    @Override
    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    @Override
    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    @Override
    public MetricsOptions getMetricsOptions() {
        return metricsOptions;
    }

    @Override
    public AuthProvider getAuthProvider() {
        return authProvider;
    }

    @Override
    public void onReady(Handler<AsyncResult<Void>> callback) {
        callback.handle(new DefaultFutureResult<>((Void) null));
    }

    @Override
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }

    @Override
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    protected void init(JsonObject config) {

        initSeeds(config.getArray(CONFIG_SEEDS));
        initPolicies(config.getObject(CONFIG_POLICIES));
        initPoolingOptions(config.getObject(CONFIG_POOLING));
        initSocketOptions(config.getObject(CONFIG_SOCKET));
        initQueryOptions(config);
        initMetricsOptions(config.getObject(CONFIG_METRICS));
        initAuthProvider(config.getObject(CONFIG_AUTH));

    }

    protected void initSeeds(JsonArray seeds) {

        // Get array of IPs, default to localhost
        if (seeds == null || seeds.size() == 0) {
            seeds = new JsonArray().addString("127.0.0.1");
        }

        this.seeds = new ArrayList<>();
        for (int i = 0; i < seeds.size(); i++) {
            this.seeds.add(seeds.<String>get(i));
        }
    }

    protected void initPolicies(JsonObject policyConfig) {

        if (policyConfig == null) {
            return;
        }

        initLoadBalancingPolicy(policyConfig.getObject("load_balancing"));
        initReconnectionPolicy(policyConfig.getObject("reconnection"));
    }

    protected void initLoadBalancingPolicy(JsonObject loadBalancing) {

        if (loadBalancing == null) {
            return;
        }

        String name = loadBalancing.getString("name");

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A load balancing policy must have a class name field");

        } else if ("DCAwareRoundRobinPolicy".equalsIgnoreCase(name)
                || "com.datastax.driver.core.policies.DCAwareRoundRobinPolicy".equalsIgnoreCase(name)) {

            String localDc = loadBalancing.getString("local_dc");
            int usedHostsPerRemoteDc = loadBalancing.getInteger("used_hosts_per_remote_dc", 0);

            if (localDc == null || localDc.isEmpty()) {
                throw new IllegalArgumentException("A DCAwareRoundRobinPolicy requires a local_dc in configuration.");
            }

            loadBalancingPolicy = new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc);

        } else {

            Class<?> clazz;
            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (LoadBalancingPolicy.class.isAssignableFrom(clazz)) {
                try {
                    loadBalancingPolicy = (LoadBalancingPolicy) clazz.newInstance();
                } catch (IllegalAccessException | InstantiationException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalArgumentException("Class " + name + " does not implement LoadBalancingPolicy");
            }

        }

    }

    protected void initReconnectionPolicy(JsonObject reconnection) {

        if (reconnection == null) {
            return;
        }

        String name = reconnection.getString("name");

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A reconnection policy must have a class name field");

        } else if ("ConstantReconnectionPolicy".equalsIgnoreCase(name) || "constant".equalsIgnoreCase(name)) {
            Integer delay = reconnection.getInteger("delay");

            if (delay == null) {
                throw new IllegalArgumentException("ConstantReconnectionPolicy requires a delay in configuration");
            }

            reconnectionPolicy = new ConstantReconnectionPolicy(delay.intValue());

        } else if ("ExponentialReconnectionPolicy".equalsIgnoreCase(name) || "exponential".equalsIgnoreCase(name)) {
            Long baseDelay = reconnection.getLong("base_delay");
            Long maxDelay = reconnection.getLong("max_delay");

            if (baseDelay == null && maxDelay == null) {
                throw new IllegalArgumentException("ExponentialReconnectionPolicy requires base_delay and max_delay in configuration");
            }

            reconnectionPolicy = new ExponentialReconnectionPolicy(baseDelay.longValue(), maxDelay.longValue());

        } else {
            Class<?> clazz;
            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (!ReconnectionPolicy.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + name + " does not implement ReconnectionPolicy");
            }

            try {
                reconnectionPolicy = (ReconnectionPolicy) clazz.newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }

    }

    protected void initPoolingOptions(JsonObject poolingConfig) {

        if (poolingConfig == null) {
            return;
        }

        poolingOptions = new PoolingOptions();

        Integer core_connections_per_host_local = poolingConfig.getInteger("core_connections_per_host_local");
        Integer core_connections_per_host_remote = poolingConfig.getInteger("core_connections_per_host_remote");
        Integer max_connections_per_host_local = poolingConfig.getInteger("max_connections_per_host_local");
        Integer max_connections_per_host_remote = poolingConfig.getInteger("max_connections_per_host_remote");
        Integer min_simultaneous_requests_local = poolingConfig.getInteger("min_simultaneous_requests_local");
        Integer min_simultaneous_requests_remote = poolingConfig.getInteger("min_simultaneous_requests_remote");
        Integer max_simultaneous_requests_local = poolingConfig.getInteger("max_simultaneous_requests_local");
        Integer max_simultaneous_requests_remote = poolingConfig.getInteger("max_simultaneous_requests_remote");

        if (core_connections_per_host_local != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, core_connections_per_host_local);
        }
        if (core_connections_per_host_remote != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, core_connections_per_host_remote);
        }
        if (max_connections_per_host_local != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, max_connections_per_host_local);
        }
        if (max_connections_per_host_remote != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, max_connections_per_host_remote);
        }
        if (min_simultaneous_requests_local != null) {
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, min_simultaneous_requests_local);
        }
        if (min_simultaneous_requests_remote != null) {
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, min_simultaneous_requests_remote);
        }
        if (max_simultaneous_requests_local != null) {
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, max_simultaneous_requests_local);
        }
        if (max_simultaneous_requests_remote != null) {
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, max_simultaneous_requests_remote);
        }

    }

    protected void initSocketOptions(JsonObject socketConfig) {

        if (socketConfig == null) {
            return;
        }

        socketOptions = new SocketOptions();

        Integer connect_timeout_millis = socketConfig.getInteger("connect_timeout_millis");
        Integer read_timeout_millis = socketConfig.getInteger("read_timeout_millis");
        Boolean keep_alive = socketConfig.getBoolean("keep_alive");
        Boolean reuse_address = socketConfig.getBoolean("reuse_address");
        Integer receive_buffer_size = socketConfig.getInteger("receive_buffer_size");
        Integer send_buffer_size = socketConfig.getInteger("send_buffer_size");
        Integer so_linger = socketConfig.getInteger("so_linger");
        Boolean tcp_no_delay = socketConfig.getBoolean("tcp_no_delay");

        if (connect_timeout_millis != null) {
            socketOptions.setConnectTimeoutMillis(connect_timeout_millis);
        }
        if (read_timeout_millis != null) {
            socketOptions.setReadTimeoutMillis(read_timeout_millis);
        }
        if (keep_alive != null) {
            socketOptions.setKeepAlive(keep_alive);
        }
        if (reuse_address != null) {
            socketOptions.setReuseAddress(reuse_address);
        }
        if (receive_buffer_size != null) {
            socketOptions.setReceiveBufferSize(receive_buffer_size);
        }
        if (send_buffer_size != null) {
            socketOptions.setSendBufferSize(send_buffer_size);
        }
        if (so_linger != null) {
            socketOptions.setSoLinger(so_linger);
        }
        if (tcp_no_delay != null) {
            socketOptions.setTcpNoDelay(tcp_no_delay);
        }

    }

    protected void initQueryOptions(JsonObject config) {

        ConsistencyLevel consistency = getConsistency(config.getString(CONFIG_CONSISTENCY_LEVEL));

        if (consistency == null) {
            return;
        }

        queryOptions = new QueryOptions().setConsistencyLevel(consistency);

    }

    protected ConsistencyLevel getConsistency(String consistency) {

        if (consistency == null || consistency.isEmpty()) {
            return null;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_ANY)) {
            return ConsistencyLevel.ANY;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_ONE)) {
            return ConsistencyLevel.ONE;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_TWO)) {
            return ConsistencyLevel.TWO;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_THREE)) {
            return ConsistencyLevel.THREE;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_QUORUM)) {
            return ConsistencyLevel.QUORUM;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_ALL)) {
            return ConsistencyLevel.ALL;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_ONE)) {
            return ConsistencyLevel.LOCAL_ONE;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_QUORUM)) {
            return ConsistencyLevel.LOCAL_QUORUM;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_EACH_QUORUM)) {
            return ConsistencyLevel.EACH_QUORUM;
        }

        throw new IllegalArgumentException("'" + consistency + "' is not a valid consistency level.");
    }

    protected void initMetricsOptions(JsonObject metrics) {

        if (metrics == null) {
            return;
        }

        boolean jmx_enabled = metrics.getBoolean("jmx_enabled", true);
        metricsOptions = new MetricsOptions(jmx_enabled);

    }

    protected void initAuthProvider(JsonObject auth) {

        if (auth == null) {
            return;
        }

        String username = auth.getString("username");
        String password = auth.getString("password");

        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalArgumentException("A username field must be provided on an auth field.");
        }
        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalArgumentException("A password field must be provided on an auth field.");
        }

        authProvider = new PlainTextAuthProvider(username, password);

    }

}
