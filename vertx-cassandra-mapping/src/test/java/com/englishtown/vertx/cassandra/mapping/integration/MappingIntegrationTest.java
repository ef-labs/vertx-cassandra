package com.englishtown.vertx.cassandra.mapping.integration;

import com.datastax.driver.core.Statement;
import com.englishtown.vertx.cassandra.integration.IntegrationTestBase;
import com.englishtown.vertx.cassandra.mapping.VertxMapper;
import com.englishtown.vertx.cassandra.mapping.VertxMappingManager;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * Cassandra driver mapping integration tests
 */
public abstract class MappingIntegrationTest extends IntegrationTestBase {

    private VertxMappingManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Statement statement = TableBuilder.create(TEST_KEYSPACE, TestEntity.TABLE_NAME)
                .ifNotExists()
                .column("id", "text")
                .column("prop1", "text")
                .column("prop2", "int")
                .primaryKey("id");

        session.execute(statement);
        manager = getInstance(VertxMappingManager.class);

    }

    @Test
    public void testMapper() throws Exception {

        VertxMapper<TestEntity> mapper = manager.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("1");
        entity.setProp1("p1");
        entity.setProp2(2);

        // Save the entity
        mapper.saveAsync(entity, callback(aVoid1 -> {

            // Get it from db
            mapper.getAsync(callback(dbEntity -> {

                assertEquals(entity.getId(), dbEntity.getId());
                assertEquals(entity.getProp1(), dbEntity.getProp1());
                assertEquals(entity.getProp2(), dbEntity.getProp2());

                // Delete the entity
                mapper.deleteAsync(dbEntity, callback(aVoid2 -> {

                    // Get it again and ensure null
                    mapper.getAsync(callback(dbEntity2 -> {

                        assertNull(dbEntity2);
                        testComplete();

                    }), entity.getId());

                }));

            }), entity.getId());

        }));

        await();

    }

    private <T> FutureCallback<T> callback(Consumer<T> onSuccess) {
        return new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                onSuccess.accept(result);
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    t.printStackTrace();
                } catch (Throwable t2) {
                }
                fail(t.getMessage());
            }
        };
    }

}
