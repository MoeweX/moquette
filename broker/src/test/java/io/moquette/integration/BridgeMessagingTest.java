package io.moquette.integration;

import io.moquette.delaygrouping.bridge.Bridge;
import io.moquette.delaygrouping.bridge.BridgeConfiguration;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class BridgeMessagingTest {

    private Bridge testInstance;
    private Bridge testInstance2;
    private List<Bridge> testInstances = new ArrayList<>();
    private Server mockServer;
    private ScheduledExecutorService mockScheduler;

    @Before
    public void setUp() {
        // Create mock objects
        mockServer = mock(Server.class);
        mockScheduler = Executors.newScheduledThreadPool(1);
    }

    @After
    public void tearDown() {
        for (Bridge bridge : testInstances) {
            bridge.shutdown();
        }
    }

    private Bridge runBridge(String port, String connections) {
        final Properties configProps = new Properties();
        configProps.setProperty("bridge_port", port);
        configProps.setProperty("bridge_host", "127.0.0.1");
        configProps.setProperty("bridge_connections", connections);

        IConfig config = new MemoryConfig(configProps);
        BridgeConfiguration bridgeConfig = new BridgeConfiguration(config);

        testInstance = new Bridge(bridgeConfig, mockServer, mockScheduler);

        testInstances.add(testInstance);
        return testInstance;
    }

    @Test
    public void testBridgeConnectUnidirectional() throws InterruptedException {
        Bridge bridge1 = runBridge("1884", "localhost:2884");
        Bridge bridge2 = runBridge("2884", "");
        Thread.sleep(5000);
        assertThat(bridge1.getConnectedBridges()).containsExactlyInAnyOrder(bridge2.getBridgeId());
        assertThat(bridge2.getConnectedBridges()).containsExactlyInAnyOrder(bridge1.getBridgeId());
    }

    @Test
    public void testBridgeConnectBidirectional() throws InterruptedException {
        Bridge bridge1 = runBridge("1884", "localhost:2884");
        Bridge bridge2 = runBridge("2884", "localhost:1884");
        Thread.sleep(5000);
        assertThat(bridge1.getConnectedBridges()).containsExactlyInAnyOrder(bridge2.getBridgeId());
        assertThat(bridge2.getConnectedBridges()).containsExactlyInAnyOrder(bridge1.getBridgeId());
    }

    @Test
    public void testBridgeConnectRing3Unidirectional() throws InterruptedException {
        Bridge bridge1 = runBridge("1884", "localhost:2884, localhost:3884");
        Bridge bridge2 = runBridge("2884", "localhost:3884");
        Bridge bridge3 = runBridge("3884", "");
        Thread.sleep(5000);
        assertThat(bridge1.getConnectedBridges()).containsExactlyInAnyOrder(bridge2.getBridgeId(), bridge3.getBridgeId());
        assertThat(bridge2.getConnectedBridges()).containsExactlyInAnyOrder(bridge1.getBridgeId(), bridge3.getBridgeId());
        assertThat(bridge3.getConnectedBridges()).containsExactlyInAnyOrder(bridge1.getBridgeId(), bridge2.getBridgeId());
    }

    @Test
    public void testBridgeConnectRing3Bidirectional() throws InterruptedException {
        Bridge bridge1 = runBridge("1884", "localhost:2884, localhost:3884");
        Bridge bridge2 = runBridge("2884", "localhost:1884, localhost:3884");
        Bridge bridge3 = runBridge("3884", "localhost:1884, localhost:2884");
        Thread.sleep(5000);
        assertThat(bridge1.getConnectedBridges()).containsExactlyInAnyOrder(bridge2.getBridgeId(), bridge3.getBridgeId());
        assertThat(bridge2.getConnectedBridges()).containsExactlyInAnyOrder(bridge1.getBridgeId(), bridge3.getBridgeId());
        assertThat(bridge3.getConnectedBridges()).containsExactlyInAnyOrder(bridge1.getBridgeId(), bridge2.getBridgeId());
    }
}
