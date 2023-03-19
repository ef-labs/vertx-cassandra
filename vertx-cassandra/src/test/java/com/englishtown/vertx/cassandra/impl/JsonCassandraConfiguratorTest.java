package com.englishtown.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.net.InetSocketAddress;
import java.util.Collection;

import static com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator.CONFIG_AUTH;
import static com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator.CONFIG_LOADER;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator}
 */
public class JsonCassandraConfiguratorTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private JsonObject config = new JsonObject();
    @Mock
    private Vertx vertx;
    @Mock
    private Context context;

    @Before
    public void setUp() {
        JsonObject baseConfig = new JsonObject().put("cassandra", config);
        when(context.config()).thenReturn(baseConfig);
        when(vertx.getOrCreateContext()).thenReturn(context);
    }

    @Test
    public void testSessionBuilder() {
        try (MockedStatic<CqlSession> session = Mockito.mockStatic(CqlSession.class)) {
            CqlSessionBuilder sessionBuilder = Mockito.mock(CqlSessionBuilder.class);
            session.when(CqlSession::builder).thenReturn(sessionBuilder);

            config.put(JsonCassandraConfigurator.CONFIG_SEEDS, new JsonArray()
                            .add("localhost:9042"))
                    .put(JsonCassandraConfigurator.CONFIG_LOCAL_DC, "test1")
                    .put(JsonCassandraConfigurator.CONFIG_AUTH, new JsonObject()
                            .put("username", "u1")
                            .put("password", "p1"));

            new JsonCassandraConfigurator(vertx);

            verify(sessionBuilder).addContactPoints(anyCollection());
            verify(sessionBuilder).withLocalDatacenter(eq("test1"));
            verify(sessionBuilder).withAuthProvider(any());
        }
    }

    @Test
    public void testInitSeeds() {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.initSeeds());

        config.put(JsonCassandraConfigurator.CONFIG_SEEDS, new JsonArray()
                .add("127.0.0.1")
                .add("127.0.0.2:9043")
                .add("127.0.0.3:9044")
        );

        configurator = new JsonCassandraConfigurator(vertx);
        Collection<InetSocketAddress> seeds = configurator.initSeeds();
        assertNotNull(seeds);
        assertEquals(3, seeds.size());

        InetSocketAddress[] addresses = seeds.toArray(new InetSocketAddress[0]);
        assertEquals("/127.0.0.1:9042", addresses[0].toString());
        assertEquals("/127.0.0.2:9043", addresses[1].toString());
        assertEquals("/127.0.0.3:9044", addresses[2].toString());

    }

    @Test
    public void testInitLocalDatacenter() {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(vertx);
        assertNull(configurator.initLocalDatacenter());

        String dc = "test1";
        config.put(JsonCassandraConfigurator.CONFIG_LOCAL_DC, dc);

        configurator = new JsonCassandraConfigurator(vertx);
        String result = configurator.initLocalDatacenter();
        assertEquals(dc, result);

    }

    @Test
    public void testInitAuthProvider() {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(config);
        AuthProvider authProvider = configurator.initAuthProvider();

        assertNull(authProvider);

        String username = "test";
        String password = "test_password";
        JsonObject auth = new JsonObject()
                .put("username", username)
                .put("password", password);

        config.put(CONFIG_AUTH, auth);

        configurator = new JsonCassandraConfigurator(config);
        authProvider = configurator.initAuthProvider();

        assertThat(authProvider, instanceOf(ProgrammaticPlainTextAuthProvider.class));

    }

    @Test
    public void testInitAuthProvider_fail() {

        String username = "test";
        String password = "test_password";

        JsonObject auth = new JsonObject();
        config.put(JsonCassandraConfigurator.CONFIG_AUTH, auth);

        try {
            new JsonCassandraConfigurator(config);
            fail("Expected error for missing username/password");
        } catch (IllegalArgumentException e) {
            // expected
        }

        auth.put("username", username);

        try {
            new JsonCassandraConfigurator(config);
            fail("Expected error for missing username/password");
        } catch (IllegalArgumentException e) {
            // expected
        }

        auth.remove("username");
        auth.put("password", password);

        try {
            new JsonCassandraConfigurator(config);
            fail("Expected error for missing username/password");
        } catch (IllegalArgumentException e) {
            // expected
        }

    }


    @Test
    public void testInitConfigLoader() {

        JsonCassandraConfigurator configurator = new JsonCassandraConfigurator(config);
        DriverConfigLoader loader = configurator.initConfigLoader();

        assertNull(loader);

        JsonObject configLoader = new JsonObject()
                .put("file", "target/test-classes/cassandra.conf");

        config.put(CONFIG_LOADER, configLoader);

        configurator = new JsonCassandraConfigurator(config);
        loader = configurator.initConfigLoader();
        assertNotNull(loader);

        DriverExecutionProfile profile = loader.getInitialConfig().getProfile("slow");
        assertNotNull(profile);
    }

}
