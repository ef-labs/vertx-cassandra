package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.englishtown.promises.*;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import org.junit.Test;
import org.vertx.java.core.Context;
import org.vertx.testtools.VertxAssert;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public class WhenCassandraSessionIntegrationTest extends IntegrationTestBase {

    @Test
    public void testExecuteAsync() throws Exception {

        final Context context = vertx.currentContext();

        Promise<ResultSet> promise = whenSession.executeAsync(createTestTableStatement);

        promise.then(
                value -> {
                    // Make sure we're on the right context
                    VertxAssert.assertEquals(context, vertx.currentContext());
                    VertxAssert.assertNotNull(value);

                    Statement statement = TableBuilder.create(keyspace, "test")
                            .column("id", "text")
                            .primaryKey("id");

                    // This promise will reject
                    return whenSession.executeAsync(statement);
                },
                value -> {
                    VertxAssert.handleThrowable(value);
                    VertxAssert.fail();
                    return null;
                }
        ).then(
                value -> {
                    // Should have reject, keyspace already exists
                    VertxAssert.fail();
                    return null;
                },
                value -> {
                    // Make sure we're on the right context
                    VertxAssert.assertEquals(context, vertx.currentContext());
                    VertxAssert.assertNotNull(value);
                    return null;
                }
        ).then(
                value -> {
                    VertxAssert.testComplete();
                    return null;
                },
                value -> {
                    VertxAssert.handleThrowable(value);
                    VertxAssert.fail();
                    return null;
                }
        );

    }

}
