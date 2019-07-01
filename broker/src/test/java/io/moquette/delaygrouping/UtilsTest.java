package io.moquette.delaygrouping;

import org.junit.Test;

import java.net.Inet4Address;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

public class UtilsTest {

    @Test
    public void getInterfaceIpv4Address() throws UnknownHostException {
        var address = Utils.getInterfaceIpv4Address("lo");
        assertThat(address).isPresent().hasValue(Inet4Address.getByName("127.0.0.1"));
    }

    @Test
    public void mqttTopicMatchesSubscription() {
        assertThat(Utils.mqttTopicMatchesSubscription("sport/blubb/players1", "sport/+/players1")).isTrue();
        assertThat(Utils.mqttTopicMatchesSubscription("sport//players1", "sport/+/players1")).isTrue();
        assertThat(Utils.mqttTopicMatchesSubscription("sport/blu/bb/players1", "sport/+/players1")).isFalse();

        assertThat(Utils.mqttTopicMatchesSubscription("sport/blubb/players1", "sport/+/players1/#")).isTrue();
        assertThat(Utils.mqttTopicMatchesSubscription("sport/blubb/players1/", "sport/+/players1/#")).isTrue();
        assertThat(Utils.mqttTopicMatchesSubscription("sport/blubb/players1/bla", "sport/+/players1/#")).isTrue();
        assertThat(Utils.mqttTopicMatchesSubscription("sport/blubb/players", "sport/+/players1/#")).isFalse();

        assertThat(Utils.mqttTopicMatchesSubscription("sport/blubb/players1/bla", "#")).isTrue();
    }
}
