package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.ResultSet;
import com.englishtown.promises.*;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
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
        Promise<ResultSet> promise = whenSession.executeAsync(createKeyspaceStatement);
        final When<ResultSet> when = new When<>();

        promise.then(
                new FulfilledRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(ResultSet value) {
                        // Make sure we're on the right context
                        VertxAssert.assertEquals(context, vertx.currentContext());
                        VertxAssert.assertNotNull(value);

                        // This promise will reject
                        return whenSession.executeAsync(createKeyspaceStatement);
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
