package io.moquette.delaygrouping;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class SubscriptionStoreTest {

    void addSubs(SubscriptionStore store) {
        store.addSubscription("client1", "sport/+/players1");
        store.addSubscription("client1", "sport/+/players1/#");
        store.addSubscription("client2", "#");
    }

    @Test
    public void matches() {
        var store = new SubscriptionStore();
        addSubs(store);

        assertThat(store.matches("blubb/bla")).isTrue();

        store.removeSubscriptions("client2");

        assertThat(store.matches("blubb/bla")).isFalse();
        assertThat(store.matches("sport/blubb/players1")).isTrue();
    }


    @Test
    public void removeSubscriptions() {
        var store = new SubscriptionStore();
        addSubs(store);

        assertThat(store.matches("blubb")).isTrue();

        store.removeSubscriptions("client2");

        assertThat(store.matches("blubb")).isFalse();
    }
}
