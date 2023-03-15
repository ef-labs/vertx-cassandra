package com.englishtown.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DefaultCassandraSession}
 */
public class DefaultCassandraSessionTest {
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private DefaultCassandraSession cassandraSession;

    @Mock
    private Vertx vertx;
    @Mock
    private CassandraConfigurator configurator;
    @Mock
    private CqlSessionBuilder sessionBuilder;
    @Mock
    private CqlSession session;
    @Mock
    private Handler<AsyncResult<Void>> callback;

    @Captor
    ArgumentCaptor<AsyncResult<Void>> callbackCaptor;
    @Captor
    ArgumentCaptor<Handler<AsyncResult<CqlSessionBuilder>>> onReadyCaptor;

    @Before
    public void setUp() {

        doAnswer(invocation -> {
            Handler<Void> handler = (Handler<Void>) invocation.getArguments()[0];
            handler.handle(null);
            return null;
        }).when(vertx).runOnContext(any());

        CompletableFuture<CqlSession> future = new CompletableFuture<>();
        when(sessionBuilder.buildAsync()).thenReturn(future);
        future.complete(session);

    }

    @Test
    public void testCtorInit() {

        cassandraSession = new DefaultCassandraSession(configurator, vertx);

        verify(configurator).onReady(onReadyCaptor.capture());
        onReadyCaptor.getValue().handle(Future.succeededFuture(sessionBuilder));

        cassandraSession.onReady(callback);
        verify(callback).handle(callbackCaptor.capture());
        assertTrue(callbackCaptor.getValue().succeeded());
    }

    @Test
    public void testCtorInitFail() {

        cassandraSession = new DefaultCassandraSession(configurator, vertx);

        Throwable cause = new RuntimeException();
        verify(configurator).onReady(onReadyCaptor.capture());
        onReadyCaptor.getValue().handle(Future.failedFuture(cause));

        cassandraSession.onReady(callback);
        verify(callback).handle(callbackCaptor.capture());
        assertEquals(cause, callbackCaptor.getValue().cause());
    }

}
