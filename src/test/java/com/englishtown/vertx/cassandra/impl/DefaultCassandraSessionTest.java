package com.englishtown.vertx.cassandra.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DefaultCassandraSession}
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCassandraSessionTest {

    DefaultCassandraSession cassandraSession;
    List<String> seeds = new ArrayList<>();

    @Mock
    Vertx vertx;
    @Mock
    Context context;
    @Mock
    CassandraConfigurator configurator;
    @Mock
    Cluster.Builder clusterBuilder;
    @Mock
    Cluster cluster;
    @Mock
    Session session;
    @Mock
    Metadata metadata;
    @Mock
    FutureCallback<ResultSet> callback;
    @Captor
    ArgumentCaptor<Statement> statementCaptor;
    @Captor
    ArgumentCaptor<Runnable> runnableCaptor;
    @Captor
    ArgumentCaptor<Handler<Void>> handlerCaptor;

    public static class TestLoadBalancingPolicy implements LoadBalancingPolicy {
        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
        }

        @Override
        public HostDistance distance(Host host) {
            return null;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return null;
        }

        @Override
        public void onAdd(Host host) {
        }

        @Override
        public void onUp(Host host) {
        }

        @Override
        public void onDown(Host host) {
        }

        @Override
        public void onRemove(Host host) {
        }
    }

    @Before
    public void setUp() {

        when(vertx.currentContext()).thenReturn(context);

        when(clusterBuilder.build()).thenReturn(cluster);
        when(cluster.connect()).thenReturn(session);
        when(cluster.getMetadata()).thenReturn(metadata);

        Provider<Cluster.Builder> provider = new Provider<Cluster.Builder>() {
            @Override
            public Cluster.Builder get() {
                return clusterBuilder;
            }
        };

        when(configurator.getConsistency()).thenReturn(LOCAL_QUORUM);
        when(configurator.getSeeds()).thenReturn(seeds);
        seeds.add("127.0.0.1");

        cassandraSession = new DefaultCassandraSession(provider, vertx);

    }

    @Test
    public void testInit() throws Exception {

        seeds.clear();
        try {
            cassandraSession.init(configurator);
            fail();
        } catch (Throwable t) {
            // Expected
        }

        seeds.add("127.0.0.1");
        seeds.add("127.0.0.2");
        seeds.add("127.0.0.3");

        LoadBalancingPolicy lbPolicy = mock(LoadBalancingPolicy.class);
        when(configurator.getLoadBalancingPolicy()).thenReturn(lbPolicy);
        PoolingOptions options = mock(PoolingOptions.class);
        when(configurator.getPoolingOptions()).thenReturn(options);

        cassandraSession.init(configurator);
        verify(clusterBuilder, times(3)).addContactPoint(anyString());
        verify(clusterBuilder).withLoadBalancingPolicy(eq(lbPolicy));
        verify(clusterBuilder).withPoolingOptions(eq(options));
        verify(clusterBuilder).build();
        verify(cluster).connect();
        verify(cluster).getMetadata();

    }

    @Test
    public void testExecuteAsync() throws Exception {

        Statement statement = mock(Statement.class);
        ResultSetFuture future = mock(ResultSetFuture.class);
        when(session.executeAsync(any(Statement.class))).thenReturn(future);

        try {
            cassandraSession.executeAsync(statement, callback);
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }

        cassandraSession.init(configurator);
        cassandraSession.executeAsync(statement, callback);
        verify(session).executeAsync(eq(statement));
        verify(future).addListener(runnableCaptor.capture(), any(Executor.class));

        ResultSet resultSet = mock(ResultSet.class);
        RuntimeException e = new RuntimeException("Unit test exception");
        when(future.get()).thenReturn(resultSet).thenThrow(e);

        runnableCaptor.getValue().run();
        verify(context).runOnContext(handlerCaptor.capture());
        handlerCaptor.getValue().handle(null);
        verify(callback).onSuccess(eq(resultSet));

        runnableCaptor.getValue().run();
        verify(context, times(2)).runOnContext(handlerCaptor.capture());
        handlerCaptor.getValue().handle(null);
        verify(callback).onFailure(eq(e));

    }

    @Test
    public void testExecuteAsync_Query() throws Exception {

        String query = "SELECT * FROM table";
        ResultSetFuture future = mock(ResultSetFuture.class);
        when(session.executeAsync(any(Statement.class))).thenReturn(future);

        try {
            cassandraSession.executeAsync(query, callback);
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }

        cassandraSession.init(configurator);
        cassandraSession.executeAsync(query, callback);
        verify(session).executeAsync(statementCaptor.capture());
        assertEquals(query, statementCaptor.getValue().toString());
        verify(future).addListener(any(Runnable.class), any(Executor.class));

    }

    @Test
    public void testExecute() throws Exception {

        String query = "SELECT * FROM table;";

        try {
            cassandraSession.execute(query);
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }

        cassandraSession.init(configurator);
        cassandraSession.execute(query);
        verify(session).execute(statementCaptor.capture());
        assertEquals(query, statementCaptor.getValue().toString());

    }

    @Test
    public void testGetMetadata() throws Exception {

        try {
            cassandraSession.getMetadata();
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }

        cassandraSession.init(configurator);
        assertEquals(metadata, cassandraSession.getMetadata());

    }

    @Test
    public void testClose() throws Exception {
        cassandraSession.init(configurator);
        cassandraSession.close();
        verify(cluster).shutdown();
    }
}
