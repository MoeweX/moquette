package io.moquette.delaygrouping;

import org.junit.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class UtilsTest {
    @Test
    public void testGetInterfaceIpv4Address() throws UnknownHostException {
        var address = Utils.getInterfaceIpv4Address("lo");
        assertThat(address).isPresent().hasValue(Inet4Address.getByName("127.0.0.1"));
    }
}
