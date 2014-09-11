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

        final WhenCassandraSession whenSession = new DefaultWhenCassandraSession(session);
        Promise<ResultSet> promise = whenSession.executeAsync(createTestTableStatement);
        final When<ResultSet> when = new When<>();

        promise.then(
                new FulfilledRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(ResultSet value) {
                        // Make sure we're on the right context
                        VertxAssert.assertEquals(context, vertx.currentContext());
                        VertxAssert.assertNotNull(value);

                        Statement statement = TableBuilder.create(keyspace, "test")
                                .column("id", "text")
                                .primaryKey("id");

                        // This promise will reject
                        return whenSession.executeAsync(statement);
                    }
                },
                new RejectedRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(Value<ResultSet> value) {
                        VertxAssert.handleThrowable(value.getCause());
                        VertxAssert.fail();
                        return null;
                    }
                }
        ).then(
                new FulfilledRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(ResultSet value) {
                        // Should have reject, keyspace already exists
                        VertxAssert.fail();
                        return null;
                    }
                },
                new RejectedRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(Value<ResultSet> value) {
                        // Make sure we're on the right context
                        VertxAssert.assertEquals(context, vertx.currentContext());
                        VertxAssert.assertNotNull(value);
                        VertxAssert.assertNotNull(value.getCause());
                        return null;
                    }
                }
        ).then(
                new FulfilledRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(ResultSet value) {
                        VertxAssert.testComplete();
                        return null;
                    }
                },
                new RejectedRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(Value<ResultSet> value) {
                        VertxAssert.handleThrowable(value.getCause());
                        VertxAssert.fail();
                        return null;
                    }
                }
        );

    }

}
