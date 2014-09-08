package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import org.vertx.java.core.Context;
import org.vertx.testtools.VertxAssert;

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
        VertxAssert.assertNotNull(rs);
        VertxAssert.testComplete();
    }

    @Test
    public void testExecuteAsync() throws Exception {

        final Context context = vertx.currentContext();

        session.executeAsync(createTestTableStatement, new FutureCallback<ResultSet>() {
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
                VertxAssert.fail();
            }
        });

    }

}
