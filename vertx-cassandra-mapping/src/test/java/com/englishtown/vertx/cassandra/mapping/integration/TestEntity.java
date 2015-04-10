package com.englishtown.vertx.cassandra.mapping.integration;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.englishtown.vertx.cassandra.integration.IntegrationTestBase;

/**
 *
 */
@Table(keyspace = IntegrationTestBase.TEST_KEYSPACE, name = TestEntity.TABLE_NAME)
public class TestEntity {

    public static final String TABLE_NAME = "test_entity";

    @PartitionKey
    private String id;
    @Column
    private String prop1;
    @Column
    private Integer prop2;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProp1() {
        return prop1;
    }

    public void setProp1(String prop1) {
        this.prop1 = prop1;
    }

    public Integer getProp2() {
        return prop2;
    }

    public void setProp2(Integer prop2) {
        this.prop2 = prop2;
    }

}
