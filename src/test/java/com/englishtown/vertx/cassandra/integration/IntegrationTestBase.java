package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.*;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

import javax.inject.Provider;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Cassandra base integration test class
 */
public abstract class IntegrationTestBase extends TestVerticle {

    protected CassandraSession session;
    protected WhenCassandraSession whenSession;
    protected String keyspace;
    protected String createKeyspaceCommand;

    public static final String TEST_CONFIG_FILE = "test_config.json";
    public static final String TEST_KEYSPACE_BASE = "test_vertx_mod_cass_";

    @Override
    public void start(Future<Void> startedResult) {

        // Load the test config file, if we have one
        JsonObject config = loadConfig();

        String dateTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        keyspace = TEST_KEYSPACE_BASE + dateTime;

        final Cluster.Builder builder = new Cluster.Builder();
        Provider<Cluster.Builder> builderProvider = new Provider<Cluster.Builder>() {
            @Override
            public Cluster.Builder get() {
                return builder;
            }
        };

        CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, container);
        session = new DefaultCassandraSession(builderProvider, configurator, vertx);
        whenSession = new DefaultWhenCassandraSession(session);

        Metadata metadata = session.getMetadata();
        if (metadata.getKeyspace(keyspace) != null) {
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
        }

        // Find out which node is closest and use that for the networktopologystrategy
        for (Host host : metadata.getAllHosts()) {
            if (session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy().distance(host) == HostDistance.LOCAL) {
                createKeyspaceCommand = "CREATE KEYSPACE " + keyspace + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + host.getDatacenter() + "' : 1 };";
                break;
            }
        }

        if (createKeyspaceCommand == null) {
            startedResult.setFailure(new Throwable("Could not find a local host for the test"));

            return;
        }

        setUp();
        startedResult.setResult(null);
        start();
    }

    /**
     * Override to run additional initialization before your integration tests run
     */
    protected void setUp() {
    }

    protected void createKeyspace() {
        ResultSet rs = session.execute(createKeyspaceCommand);
        VertxAssert.assertNotNull(rs);
    }

    protected void dropKeyspace() {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
    }

    private JsonObject loadConfig() {
        try {
            return new JsonObject(Resources.toString(Resources.getResource(TEST_CONFIG_FILE), Charsets.UTF_8));
        } catch (Exception e) {
            return new JsonObject();
        }
    }

    /**
     * Vert.x calls the stop method when the verticle is undeployed.
     * Put any cleanup code for your verticle in here
     */
    @Override
    public void stop() {
        try {
            if (session != null) {
                dropKeyspace();
                session.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
