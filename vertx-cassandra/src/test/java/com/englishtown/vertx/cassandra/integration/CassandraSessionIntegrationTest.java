package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import io.vertx.core.Context;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public class CassandraSessionIntegrationTest extends IntegrationTestBase {

    @Test
    public void testExecute() throws Exception {

        session.execute(createTestTableStatement);

        RegularStatement statement = QueryBuilder
                .select()
                .from(keyspace, "test")
                .where(QueryBuilder.eq("id", QueryBuilder.bindMarker()));

        PreparedStatement prepared = session.prepare(statement);

        BoundStatement bound = prepared.bind("123");
        ResultSet rs = session.execute(bound);
        assertNotNull(rs);
        testComplete();
    }

    @Test
    public void testExecuteAsync() throws Exception {

        vertx.runOnContext(aVoid -> {

            Context context = vertx.getOrCreateContext();

            session.executeAsync(createTestTableStatement, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet result) {
                    // Make sure we're on the right context
                    assertEquals(context, vertx.getOrCreateContext());
                    assertNotNull(result);
                    testComplete();
                }

                @Override
                public void onFailure(Throwable t) {
                    handleThrowable(t);
                }
            });

        });

        await();

    }

}
