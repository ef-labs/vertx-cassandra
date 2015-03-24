package com.englishtown.vertx.cassandra.mapping.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.MappingManager;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.mapping.VertxMapper;
import com.englishtown.vertx.cassandra.mapping.integration.TestEntity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultVertxMapper}
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultVertxMappingManagerTest {

    private DefaultVertxMappingManager manager;

    @Mock
    private CassandraSession session;
    @Mock
    private Cluster cluster;
    @Mock
    private Metadata metadata;
    @Mock
    private KeyspaceMetadata keyspaceMetadata;
    @Mock
    private Configuration configuration;
    @Mock
    private ProtocolOptions protocolOptions;

    @Before
    public void setUp() throws Exception {

        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(anyString())).thenReturn(keyspaceMetadata);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersionEnum()).thenReturn(ProtocolVersion.NEWEST_SUPPORTED);

        manager = new DefaultVertxMappingManager(session);

    }

    @Test
    public void testGetMappingManager() throws Exception {

        assertThat(manager.getMappingManager(), instanceOf(MappingManager.class));

    }

    @Test
    public void testMapper() throws Exception {

        VertxMapper<TestEntity> mapper = manager.mapper(TestEntity.class);

        assertNotNull(mapper);
        assertNotNull(mapper.getMapper());

    }

}