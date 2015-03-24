package com.englishtown.vertx.cassandra.mapping.integration;

import com.datastax.driver.core.Statement;
import com.englishtown.vertx.cassandra.integration.IntegrationTestBase;
import com.englishtown.vertx.cassandra.mapping.hk2.WhenHK2MappingBinder;
import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMapper;
import com.englishtown.vertx.cassandra.mapping.promises.WhenVertxMappingManager;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.junit.Test;

/**
 * Cassandra driver mapping integration tests
 */
public class WhenMappingIntegrationTest extends IntegrationTestBase {

    private WhenVertxMappingManager manager;

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

        ServiceLocatorUtilities.bind(locator, new WhenHK2MappingBinder());
        manager = locator.getService(WhenVertxMappingManager.class);

    }

    @Test
    public void testMapper() throws Exception {

        WhenVertxMapper<TestEntity> mapper = manager.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("1");
        entity.setProp1("p1");
        entity.setProp2(2);

        mapper.saveAsync(entity)
                .then(aVoid -> mapper.getAsync(entity.getId()))
                .then(dbEntity -> {
                    assertEquals(entity.getId(), dbEntity.getId());
                    assertEquals(entity.getProp1(), dbEntity.getProp1());
                    assertEquals(entity.getProp2(), dbEntity.getProp2());

                    return mapper.deleteAsync(dbEntity);
                })
                .then(aVoid -> mapper.getAsync(entity.getId()))
                .then(dbEntity -> {
                    assertNull(dbEntity);
                    testComplete();
                    return null;
                })
                .otherwise(t -> {
                    try {
                        t.printStackTrace();
                    } catch (Throwable t2) {
                    }
                    fail(t.getMessage());
                    return null;
                });

        await();

    }

}
