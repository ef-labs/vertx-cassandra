package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.keyspacebuilder.KeyspaceBuilder;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.tablebuilder.CreateTable;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;

import java.util.concurrent.CountDownLatch;

/**
 * Cassandra base integration test class
 */
public abstract class IntegrationTestBase extends VertxTestBase {

    private Locator locator;
    protected CassandraSession session;
    protected WhenCassandraSession whenSession;
    protected String keyspace;
    protected CreateTable createTestTableStatement;
    protected When when;

    public static final String TEST_CONFIG_FILE = "test_config.json";
    public static final String TEST_KEYSPACE = "test_vertx_mod_cass";


    protected abstract Locator createLocator();

    /**
     * Override to run additional initialization before your integration tests run
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        locator = createLocator();

        CountDownLatch latch = new CountDownLatch(1);

        vertx.runOnContext(aVoid -> {

            // Load the test config file, if we have one
            JsonObject config = loadConfig();
            vertx.getOrCreateContext().config().mergeIn(config);

//        String dateTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            keyspace = TEST_KEYSPACE; //dateTime;

            session = getInstance(CassandraSession.class);
            when = getInstance(When.class);
            whenSession = getInstance(WhenCassandraSession.class);

            session.onReady(result -> {
                if (result.failed()) {
                    result.cause().printStackTrace();
                    fail();
                    latch.countDown();
                    return;
                }

                Metadata metadata = session.getMetadata();
                KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
                if (keyspaceMetadata != null) {
                    for (TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
                        session.execute(TableBuilder.drop(keyspace, tableMetadata.getName()).ifExists());
                    }
                } else {
                    createKeyspace(metadata);
                }

                createTestTableStatement = TableBuilder.create(keyspace, "test")
                        .ifNotExists()
                        .column("id", "text")
                        .column("value", "text")
                        .primaryKey("id");

                latch.countDown();
            });
        });

        latch.await();
    }

    protected <T> T getInstance(Class<T> clazz) {
        return locator.getInstance(clazz);
    }

    private void createKeyspace(Metadata metadata) {

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
            fail("Could not find a local host for the test");
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
    public void tearDown() throws Exception {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        super.tearDown();
    }

    protected void handleThrowable(Throwable t) {
        t.printStackTrace();
        fail();
    }

}
