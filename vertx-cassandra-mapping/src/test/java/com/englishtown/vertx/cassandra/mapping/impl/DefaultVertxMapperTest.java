package com.englishtown.vertx.cassandra.mapping.impl;

import com.datastax.driver.mapping.Mapper;
import com.englishtown.vertx.cassandra.mapping.integration.TestEntity;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Vertx;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultVertxMapper}
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultVertxMapperTest {

    private DefaultVertxMapper<TestEntity> mapper;
    private TestEntity entity = new TestEntity();

    @Mock
    private Mapper<TestEntity> rawMapper;
    @Mock
    private Vertx vertx;
    @Mock
    private FutureCallback<Void> voidCallback;
    @Mock
    private FutureCallback<TestEntity> entityCallback;
    @Mock
    private ListenableFuture<Void> voidFuture;
    @Mock
    private ListenableFuture<TestEntity> entityFuture;

    @Before
    public void setUp() throws Exception {

        mapper = new DefaultVertxMapper<>(rawMapper, vertx);

        when(rawMapper.saveAsync(eq(entity))).thenReturn(voidFuture);
        when(rawMapper.deleteAsync(eq(entity))).thenReturn(voidFuture);
        when(rawMapper.deleteAsync(any(), any())).thenReturn(voidFuture);
        when(rawMapper.getAsync(any(), any())).thenReturn(entityFuture);

    }

    @Test
    public void testGetMapper() throws Exception {
        assertEquals(rawMapper, mapper.getMapper());
    }

    @Test
    public void testSaveAsync() throws Exception {

        mapper.saveAsync(entity, voidCallback);
        verify(rawMapper).saveAsync(eq(entity));
        verify(voidFuture).addListener(any(), any());

    }

    @Test
    public void testDeleteAsync() throws Exception {

        mapper.deleteAsync(entity, voidCallback);
        verify(rawMapper).deleteAsync(eq(entity));
        verify(voidFuture).addListener(any(), any());

    }

    @Test
    public void testDeleteAsync_ById() throws Exception {

        Object key1 = new Object();
        Object key2 = new Object();

        mapper.deleteAsync(voidCallback, key1, key2);
        verify(rawMapper).deleteAsync(eq(key1), eq(key2));
        verify(voidFuture).addListener(any(), any());

    }

    @Test
    public void testGetAsync() throws Exception {

        Object key1 = new Object();
        Object key2 = new Object();

        mapper.getAsync(entityCallback, key1, key2);
        verify(rawMapper).getAsync(eq(key1), eq(key2));
        verify(entityFuture).addListener(any(), any());

    }

}