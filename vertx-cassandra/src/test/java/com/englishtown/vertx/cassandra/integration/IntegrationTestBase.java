package com.englishtown.vertx.cassandra.integration;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.BeforeClass;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Cassandra base integration test class
 */
public abstract class IntegrationTestBase extends VertxTestBase {

    private Locator locator;
    protected CassandraSession session;
    protected WhenCassandraSession whenSession;
    protected String keyspace;
    protected When when;
    protected SimpleStatement createTestTableStatement;

    public static final String TEST_CONFIG_FILE = "test_config.json";
    public static final String TEST_KEYSPACE = "test_vertx_mod_cass";

    private static EmbeddedCassandraService cassandraService = null;

    protected abstract Locator createLocator();

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (cassandraService == null) {
            String embedded = System.getProperty("test.embedded", "");
            if (!"true".equals(embedded)) {
                return;
            }
            System.setProperty("cassandra.storagedir", "target/cassandra");
            cassandraService = new EmbeddedCassandraService();
            cassandraService.start();
        }
    }

    /**
     * Override to run additional initialization before your integration tests run
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        locator = createLocator();

        CompletableFuture<Void> future = new CompletableFuture<>();

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
                    future.completeExceptionally(result.cause());
                    return;
                }

                Metadata metadata = session.getMetadata();
                Optional<KeyspaceMetadata> keyspaceMetadata = metadata.getKeyspace(keyspace);
                if (keyspaceMetadata.isPresent()) {
                    for (CqlIdentifier tableName : keyspaceMetadata.get().getTables().keySet()) {
                        SimpleStatement dropTable = SchemaBuilder.dropTable(keyspace, tableName.toString()).build();
                        session.getSession().execute(dropTable);
                    }
                } else {
                    createKeyspace(metadata);
                }

                createTestTableStatement = SchemaBuilder.createTable(keyspace, "test")
                        .ifNotExists()
                        .withPartitionKey("id", DataTypes.TEXT)
                        .withColumn("value", DataTypes.TEXT)
                        .build();

                future.complete(null);
            });
        });

        future.get();
    }

    protected <T> T getInstance(Class<T> clazz) {
        return locator.getInstance(clazz);
    }

    private void createKeyspace(Metadata metadata) {

        SimpleStatement createKeyspace = SchemaBuilder
                .createKeyspace(keyspace)
                .ifNotExists()
                .withSimpleStrategy(1)
                .build();

        ResultSet rs = session.getSession().execute(createKeyspace);

        if (!rs.wasApplied()) {
            throw new RuntimeException("Keyspace was not created");
        }

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
