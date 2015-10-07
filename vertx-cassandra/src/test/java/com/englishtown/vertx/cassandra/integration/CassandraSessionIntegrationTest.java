package com.englishtown.vertx.cassandra.integration;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import io.vertx.core.Context;
import org.junit.Test;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.CassandraSession}
 */
public abstract class CassandraSessionIntegrationTest extends IntegrationTestBase {

    @Test
    public void testExecute() throws Exception {

        session.execute(createTestTableStatement);

        RegularStatement statement = new QueryBuilder(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE)
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
