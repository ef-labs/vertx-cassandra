package com.englishtown.vertx.cassandra.promises.impl;

import com.englishtown.promises.*;
import com.englishtown.vertx.cassandra.CassandraSession;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link DefaultWhenCassandraSession}
 */
public class DefaultWhenCassandraSessionTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private CassandraSession cassandraSession;
    @Mock
    private Vertx vertx;
    @Captor
    private ArgumentCaptor<Handler<AsyncResult<Void>>> onReadyCaptor;

    private When when;
    private DefaultWhenCassandraSession whenCassandraSession;

    @Before
    public void setUp() throws Exception {
        when = WhenFactory.createSync();
        whenCassandraSession = new DefaultWhenCassandraSession(cassandraSession, when, vertx);
    }

    @Test
    public void testReady() throws Exception {

        Promise<Void> p = whenCassandraSession.ready();
        State<Void> state = p.inspect();
        assertEquals(HandlerState.PENDING, state.getState());

        verify(cassandraSession).onReady(onReadyCaptor.capture());
        onReadyCaptor.getValue().handle(Future.succeededFuture());

        state = p.inspect();
        assertEquals(HandlerState.FULFILLED, state.getState());

    }

    @Test
    public void testReadyReject() throws Exception {

        Promise<Void> p = whenCassandraSession.ready();
        State<Void> state = p.inspect();
        assertEquals(HandlerState.PENDING, state.getState());

        verify(cassandraSession).onReady(onReadyCaptor.capture());
        onReadyCaptor.getValue().handle(Future.failedFuture("Test fail"));

        state = p.inspect();
        assertEquals(HandlerState.REJECTED, state.getState());

    }

}