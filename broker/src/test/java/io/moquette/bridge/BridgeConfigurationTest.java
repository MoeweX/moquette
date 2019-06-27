package io.moquette.bridge;

import io.moquette.bridge.BridgeConfiguration;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class BridgeConfigurationTest {

    private Properties propsFromString(String s) {
        Properties props = new Properties();
        try {
            props.load(new StringReader(s));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    @Test
    public void testSimpleConfig() {
        IConfig config = new MemoryConfig(propsFromString(
            "bridge_host 0.0.0.0\n" +
                "bridge_port 1884\n" +
                "bridge_connections \\\n" +
                "123.12.1.234:1234,\\"));
        BridgeConfiguration bridgeConfig = new BridgeConfiguration(config);

        assertEquals(Arrays.asList(new InetSocketAddress("123.12.1.234", 1234)), bridgeConfig.getBridgePeers());
    }

}
