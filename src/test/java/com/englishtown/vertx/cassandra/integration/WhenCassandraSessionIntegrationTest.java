package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.Statement;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import io.vertx.core.Context;
import org.junit.Test;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public class WhenCassandraSessionIntegrationTest extends IntegrationTestBase {

    @Test
    public void testExecuteAsync() throws Exception {

        vertx.runOnContext(aVoid -> {

            final Context context = vertx.getOrCreateContext();

            whenSession.executeAsync(createTestTableStatement)
                    .then(value -> {
                        // Make sure we're on the right context
                        assertEquals(context, vertx.getOrCreateContext());
                        assertNotNull(value);

                        Statement statement = TableBuilder.create(keyspace, "test")
                                .column("id", "text")
                                .primaryKey("id");

                        // This promise will reject
                        return whenSession.executeAsync(statement);
                    })
                    .then(value -> {
                                // Should have reject, keyspace already exists
                                fail();
                                return null;
                            },
                            value -> {
                                // Make sure we're on the right context
                                assertEquals(context, vertx.getOrCreateContext());
                                assertNotNull(value);
                                return null;
                            })
                    .then(value -> {
                        testComplete();
                        return null;
                    })
                    .otherwise(t -> {
                        handleThrowable(t);
                        return null;
                    });

        });

        await();
    }

}
