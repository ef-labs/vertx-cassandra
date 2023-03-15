package com.englishtown.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.FutureUtils;
import com.google.common.util.concurrent.FutureCallback;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Default implementation of {@link CassandraSession}
 */
public class DefaultCassandraSession implements CassandraSession {

    private final Vertx vertx;
    private List<Handler<AsyncResult<Void>>> onReadyCallbacks = new ArrayList<>();

    protected CqlSession session;
    protected CassandraConfigurator configurator;
    protected AsyncResult<Void> initResult;

    private final Logger logger = LoggerFactory.getLogger(DefaultCassandraSession.class);

    @Inject
    public DefaultCassandraSession(CassandraConfigurator configurator, Vertx vertx) {
        this.configurator = configurator;
        this.vertx = vertx;

        configurator.onReady(result -> {
            if (result.failed()) {
                initResult = Future.failedFuture(result.cause());
                runOnReadyCallbacks();
                return;
            }
            try {
                init(result.result());
            } catch (Throwable t) {
                initResult = Future.failedFuture(t);
                runOnReadyCallbacks();
            }
        });
    }

    protected void init(CqlSessionBuilder sessionBuilder) {
        // Build session
        sessionBuilder.buildAsync()
                .thenAccept(s -> {
                    session = s;
                    initResult = Future.succeededFuture();
                })
                .exceptionally(t -> {
                    initResult = Future.failedFuture(t);
                    return null;
                })
                .thenRun(this::runOnReadyCallbacks);
    }

    private void runOnReadyCallbacks() {
        vertx.runOnContext(aVoid -> {
            onReadyCallbacks.forEach(handler -> handler.handle(initResult));
            onReadyCallbacks.clear();
        });
    }

    @NonNull
    @Override
    public String getName() {
        return getSession().getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metadata getMetadata() {
        return getSession().getMetadata();
    }

    @Override
    public boolean isSchemaMetadataEnabled() {
        return getSession().isSchemaMetadataEnabled();
    }

    @NonNull
    @Override
    public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
        return getSession().setSchemaMetadataEnabled(newValue);
    }

    @NonNull
    @Override
    public CompletionStage<Metadata> refreshSchemaAsync() {
        return getSession().refreshSchemaAsync();
    }

    @NonNull
    @Override
    public CompletionStage<Boolean> checkSchemaAgreementAsync() {
        return getSession().checkSchemaAgreementAsync();
    }

    @NonNull
    @Override
    public DriverContext getContext() {
        return getSession().getContext();
    }

    @NonNull
    @Override
    public Optional<CqlIdentifier> getKeyspace() {
        return getSession().getKeyspace();
    }

    @NonNull
    @Override
    public Optional<Metrics> getMetrics() {
        return getSession().getMetrics();
    }

    @Nullable
    @Override
    public <RequestT extends Request, ResultT> ResultT execute(@NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
        return getSession().execute(request, resultType);
    }

    @NonNull
    @Override
    public CompletionStage<Void> closeFuture() {
        return getSession().closeFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return (session == null || session.isClosed());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean initialized() {
        return initResult != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReady(Handler<AsyncResult<Void>> callback) {
        if (initResult != null) {
            callback.handle(initResult);
        } else {
            onReadyCallbacks.add(callback);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CqlSession getSession() {
        checkInitialized();
        return session;
    }

    /**
     * Returns the vert.x instance
     *
     * @return
     */
    @Override
    public Vertx getVertx() {
        return vertx;
    }

    /**
     * Executes a cassandra statement asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param statement the statement to execute
     * @param callback  the callback for on completion
     */
    @Override
    public void executeAsync(Statement<?> statement, FutureCallback<AsyncResultSet> callback) {
        addCallback(getSession().executeAsync(statement), callback);
    }

    /**
     * Executes a cassandra CQL query asynchronously.  Ensures the callback is executed on the correct vert.x context.
     *
     * @param query    the CQL query to execute
     * @param callback the callback for on completion
     */
    @Override
    public void executeAsync(String query, FutureCallback<AsyncResultSet> callback) {
        addCallback(executeAsync(query), callback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<PreparedStatement> prepareAsync(String query) {
        return getSession().prepareAsync(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<PreparedStatement> prepareAsync(SimpleStatement statement) {
        return getSession().prepareAsync(statement);
    }

    /**
     * Prepares the provided query statement
     *
     * @param statement the query statement to prepare
     * @param callback  the callback for on completion
     */
    @Override
    public void prepareAsync(SimpleStatement statement, FutureCallback<PreparedStatement> callback) {
        addCallback(prepareAsync(statement), callback);
    }

    /**
     * Prepares the provided query
     *
     * @param query    the query to prepare
     * @param callback the callback for on completion
     */
    @Override
    public void prepareAsync(String query, FutureCallback<PreparedStatement> callback) {
        addCallback(prepareAsync(query), callback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<Void> closeAsync() {
        return getSession().closeAsync();
    }

    @NonNull
    @Override
    public CompletionStage<Void> forceCloseAsync() {
        return this.getSession().forceCloseAsync();
    }

    @Override
    public void close() {
        logger.debug("Call to close the session has been made");
        if (session != null) {
            session.forceCloseAsync();
            session = null;
        }
    }

    private <V> void addCallback(final CompletionStage<V> future, FutureCallback<V> callback) {
        FutureUtils.addCallback(future, callback, vertx);
    }

    private void checkInitialized() {
        if (!initialized()) {
            throw new IllegalStateException("Cassandra session is not ready for use yet");
        }
    }

}