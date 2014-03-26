package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import org.vertx.java.core.Context;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

import javax.inject.Provider;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public class CassandraSessionIntegrationTest extends TestVerticle {

    CassandraSession session;

    private static final String TEST_KEYSPACE_BASE = "test_vertx_mod_cass_";
    private String keyspace;
    private String createKeyspaceCommand;

    @Test
    public void testExecute() throws Exception {

        ResultSet rs = session.execute(createKeyspaceCommand);
        VertxAssert.assertNotNull(rs);

        session.execute("CREATE TABLE " + keyspace + ".test (id text PRIMARY KEY, value text)");

        RegularStatement statement = QueryBuilder
                .select()
                .from(keyspace, "test")
                .where(QueryBuilder.eq("id", QueryBuilder.bindMarker()));

        PreparedStatement prepared = session.prepare(statement);

        BoundStatement bound = prepared.bind("123");
        rs = session.execute(bound);
        VertxAssert.assertNotNull(rs);

        session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
        VertxAssert.testComplete();
    }

    @Test
    public void testExecuteAsync() throws Exception {

        final Context context = vertx.currentContext();

        session.executeAsync(createKeyspaceCommand, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                // Make sure we're on the right context
                VertxAssert.assertEquals(context, vertx.currentContext());
                VertxAssert.assertNotNull(result);
                session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
                VertxAssert.testComplete();
            }

            @Override
            public void onFailure(Throwable t) {
                session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
                VertxAssert.handleThrowable(t);
            }
        });

    }

    @Override
    public void start(Future<Void> startedResult) {

        String dateTime = new SimpleDateFormat("yyMMddHHmmss").format(new Date());
        keyspace = TEST_KEYSPACE_BASE + dateTime;
        createKeyspaceCommand = "CREATE KEYSPACE " + keyspace + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

        final Cluster.Builder builder = new Cluster.Builder();
        Provider<Cluster.Builder> builderProvider = new Provider<Cluster.Builder>() {
            @Override
            public Cluster.Builder get() {
                return builder;
            }
        };

        CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(buildConfigFromEnvVars(), container);
        session = new DefaultCassandraSession(builderProvider, configurator, vertx);

        Metadata metadata = session.getMetadata();
        if (metadata.getKeyspace(keyspace) != null) {
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
        }

        startedResult.setResult(null);
        start();
    }

    /**
     * Vert.x calls the stop method when the verticle is undeployed.
     * Put any cleanup code for your verticle in here
     */
    @Override
    public void stop() {
        try {
            session.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonObject buildConfigFromEnvVars() {

        JsonObject cassConfig = new JsonObject();

        // Grab load balancing policy & consistency level
        String loadBalancingNameEnv = container.env().get("CASSANDRA_POLICIES_LOADBALANCING_NAME");
        String loadBalancingLocalDCEnv = container.env().get("CASSANDRA_POLICIES_LOCAL_DC");

        if (!Strings.isNullOrEmpty(loadBalancingNameEnv) && !Strings.isNullOrEmpty(loadBalancingLocalDCEnv)) {
            JsonObject loadBalancing = new JsonObject().putString("name", loadBalancingNameEnv).putString("local_dc", loadBalancingLocalDCEnv);
            JsonObject policies = new JsonObject().putObject("load_balancing", loadBalancing);

            cassConfig.putObject("policies", policies);
        }

        // And the consistency level
        String consistencyLevelEnv = container.env().get("CASSANDRA_CONSISTENCY_LEVEL");

        if (!Strings.isNullOrEmpty(consistencyLevelEnv)) {
            cassConfig.putString("consistency_level", consistencyLevelEnv);
        }

        return cassConfig;
    }
}
