package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import org.vertx.java.core.Context;
import org.vertx.java.core.Future;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

import javax.inject.Provider;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public class CassandraSessionIntegrationTest extends TestVerticle {

    CassandraSession session;

    private static final String TEST_KEYSPACE = "et_vertx_mod_cassandra_integration_test";
    private static final String CQL_CREATE_KEYSPACE = "CREATE KEYSPACE et_vertx_mod_cassandra_integration_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

    @Test
    public void testExecute() throws Exception {

        ResultSet rs = session.execute(CQL_CREATE_KEYSPACE);
        VertxAssert.assertNotNull(rs);
        VertxAssert.testComplete();

    }

    @Test
    public void testExecuteAsync() throws Exception {

        final Context context = vertx.currentContext();

        session.executeAsync(CQL_CREATE_KEYSPACE, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                // Make sure we're on the right context
                VertxAssert.assertEquals(context, vertx.currentContext());
                VertxAssert.assertNotNull(result);
                VertxAssert.testComplete();
            }

            @Override
            public void onFailure(Throwable t) {
                VertxAssert.handleThrowable(t);
            }
        });

    }

    @Override
    public void start(Future<Void> startedResult) {

        final Cluster.Builder builder = new Cluster.Builder();
        Provider<Cluster.Builder> builderProvider = new Provider<Cluster.Builder>() {
            @Override
            public Cluster.Builder get() {
                return builder;
            }
        };

        CassandraConfigurator configurator = new JsonCassandraConfigurator(container);
        session = new DefaultCassandraSession(builderProvider, configurator, vertx);

        Metadata metadata = session.getMetadata();
        if (metadata.getKeyspace(TEST_KEYSPACE) != null) {
            session.execute("DROP KEYSPACE " + TEST_KEYSPACE + ";");
        }

        startedResult.setResult(null);
        start();
    }
}
