package com.englishtown.vertx.cassandra.impl;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.net.InetSocketAddress;
import java.util.Collection;

import static com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator.CONFIG_CASSANDRA;
import static com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator.CONFIG_SEEDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator}
 */
public class EnvironmentCassandraConfiguratorTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    private JsonObject config = new JsonObject();
    private JsonObject cassConfig;

    @Mock
    private Vertx vertx;
    @Mock
    private Context context;

    @Before
    public void setUp() {
        when(context.config()).thenReturn(config);
        when(vertx.getOrCreateContext()).thenReturn(context);

        config.put(CONFIG_CASSANDRA, cassConfig = new JsonObject());
        cassConfig.put(CONFIG_SEEDS, new JsonArray().add("127.0.0.1:9042"));
    }


    @Test
    public void testInitSeeds_Defaults() {

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(vertx);
        Collection<InetSocketAddress> seeds = configurator.initSeeds();

        assertNotNull(seeds);
        assertEquals(1, seeds.size());
        assertEquals("/127.0.0.1:9042", seeds.toArray()[0].toString());
    }

    @Test
    public void testInitSeeds() {

        environmentVariables.set(EnvironmentCassandraConfigurator.ENV_VAR_SEEDS, "127.0.0.2|127.0.0.3:9043|127.0.0.4:9044");

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(vertx);
        Collection<InetSocketAddress> seeds = configurator.initSeeds();

        assertNotNull(seeds);
        InetSocketAddress[] addresses = seeds.toArray(new InetSocketAddress[0]);
        assertEquals(3, seeds.size());
        assertEquals("/127.0.0.2:9042", addresses[0].toString());
        assertEquals("/127.0.0.3:9043", addresses[1].toString());
        assertEquals("/127.0.0.4:9044", addresses[2].toString());
    }

    @Test
    public void testInitLocalDatacenter() {

        JsonCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(vertx);
        assertNull(configurator.initLocalDatacenter());

        String dc = "test1";
        environmentVariables.set(EnvironmentCassandraConfigurator.ENV_VAR_LOCAL_DC, dc);

        configurator = new EnvironmentCassandraConfigurator(vertx);
        String result = configurator.initLocalDatacenter();
        assertEquals(dc, result);

    }

    @Test
    public void testInitAuthProvider() {

        EnvironmentCassandraConfigurator configurator = new EnvironmentCassandraConfigurator(config);
        AuthProvider authProvider = configurator.initAuthProvider();

        assertNull(authProvider);

        String username = "test";
        String password = "test_password";
        environmentVariables.set(EnvironmentCassandraConfigurator.ENV_VAR_USERNAME, username);
        environmentVariables.set(EnvironmentCassandraConfigurator.ENV_VAR_PASSWORD, password);

        configurator = new EnvironmentCassandraConfigurator(config);
        authProvider = configurator.initAuthProvider();

        assertThat(authProvider, instanceOf(ProgrammaticPlainTextAuthProvider.class));

    }
}
