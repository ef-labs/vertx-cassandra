package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.englishtown.vertx.cassandra.keyspacebuilder.KeyspaceBuilder;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import com.englishtown.vertx.cassandra.tablebuilder.CreateTable;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

/**
 * Cassandra base integration test class
 */
public abstract class IntegrationTestBase extends TestVerticle {

    protected CassandraSession session;
    protected WhenCassandraSession whenSession;
    protected String keyspace;
    protected CreateTable createTestTableStatement;

    public static final String TEST_CONFIG_FILE = "test_config.json";
    public static final String TEST_KEYSPACE_BASE = "test_vertx_mod_cass_";

    @Override
    public void start(Future<Void> startedResult) {

        // Load the test config file, if we have one
        JsonObject config = loadConfig();

//        String dateTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        keyspace = TEST_KEYSPACE_BASE + "1"; //dateTime;

        final Cluster.Builder builder = new Cluster.Builder();

        CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config, container);
        session = new DefaultCassandraSession(builder, configurator, vertx);
        whenSession = new DefaultWhenCassandraSession(session);

        Metadata metadata = session.getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        if (keyspaceMetadata != null) {
            for (TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
                session.execute(TableBuilder.drop(keyspace, tableMetadata.getName()).ifExists());
            }
        } else {
            createKeyspace(metadata, startedResult);
        }

        createTestTableStatement = TableBuilder.create(keyspace, "test")
                .ifNotExists()
                .column("id", "text")
                .column("value", "text")
                .primaryKey("id");

        setUp();
        startedResult.setResult(null);
        start();
    }

    /**
     * Override to run additional initialization before your integration tests run
     */
    protected void setUp() {
    }

    private void createKeyspace(Metadata metadata, Future<Void> startedResult) {

        Statement createKeyspaceStatement = null;

        // Find out which node is closest and use that for the networktopologystrategy
        LoadBalancingPolicy lbPolicy = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        for (Host host : metadata.getAllHosts()) {
            if (lbPolicy.distance(host) == HostDistance.LOCAL) {
                createKeyspaceStatement = KeyspaceBuilder.create(keyspace)
                        .ifNotExists()
                        .networkTopologyStrategy()
                        .dc(host.getDatacenter(), 1);
                break;
            }
        }

        if (createKeyspaceStatement == null) {
            startedResult.setFailure(new Throwable("Could not find a local host for the test"));
            return;
        }

        session.execute(createKeyspaceStatement);

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
                session.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
