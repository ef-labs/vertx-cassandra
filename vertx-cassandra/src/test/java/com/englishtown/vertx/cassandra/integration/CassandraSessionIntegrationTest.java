package com.englishtown.vertx.cassandra.integration;

import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.google.common.util.concurrent.FutureCallback;
import io.vertx.core.Context;
import org.junit.Test;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public abstract class CassandraSessionIntegrationTest extends IntegrationTestBase {

    @Test
    public void testExecute() {

        session.getSession().execute(createTestTableStatement);

        SimpleStatement statement = QueryBuilder
                .selectFrom(keyspace, "test")
                .all()
                .where(Relation.column("id").isEqualTo(QueryBuilder.bindMarker()))
                .build();

        PreparedStatement prepared = session.getSession().prepare(statement);

        BoundStatement bound = prepared.bind("123");
        ResultSet rs = session.getSession().execute(bound);
        assertNotNull(rs);
        testComplete();
    }

    @Test
    public void testExecuteAsync() {

        vertx.runOnContext(aVoid -> {

            Context context = vertx.getOrCreateContext();

            session.executeAsync(createTestTableStatement, new FutureCallback<AsyncResultSet>() {
                @Override
                public void onSuccess(AsyncResultSet result) {
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
