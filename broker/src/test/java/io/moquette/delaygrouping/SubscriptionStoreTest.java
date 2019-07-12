package io.moquette.delaygrouping;

import io.moquette.spi.impl.subscriptions.Subscription;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class SubscriptionStoreTest {

    void addSubs(SubscriptionStore store) {
        store.addSubscription("client1", "sport/+/players1");
        store.addSubscription("client1", "sport/+/players1/#");
    }

    void addSubs2(SubscriptionStore store) {
        store.addSubscription("client3", "test/whatever");
    }

    void addSubsAll(SubscriptionStore store) {
        store.addSubscription("client2", "#");
    }

    @Test
    public void matches() {
        var store = new SubscriptionStore();
        addSubs(store);

        assertThat(store.matches("blubb/bla")).isFalse();

        addSubsAll(store);

        assertThat(store.matches("blubb/bla")).isTrue();

        store.removeSubscriptions("client2");

        assertThat(store.matches("blubb/bla")).isFalse();
        assertThat(store.matches("sport/blubb/players1")).isTrue();
    }

    @Test
    public void mergeSubscriptions() {
        var store1 = new SubscriptionStore();
        addSubs(store1);

        assertThat(store1.matches("sport/blubb/players1")).isTrue();
        assertThat(store1.matches("test/whatever")).isFalse();

        var store2 = new SubscriptionStore();
        addSubs2(store2);

        assertThat(store2.matches("sport/blubb/players1")).isFalse();
        assertThat(store2.matches("test/whatever")).isTrue();

        store1.merge(store2);
        assertThat(store1.matches("sport/blubb/players1")).isTrue();
        assertThat(store1.matches("test/whatever")).isTrue();
    }

    @Test
    public void copyConstructor() {
        var store1 = new SubscriptionStore();
        addSubs(store1);

        assertThat(store1.matches("sport/blubb/players1")).isTrue();
        assertThat(store1.matches("test/whatever")).isFalse();

        var store2 = new SubscriptionStore(store1);
        assertThat(store2.matches("sport/blubb/players1")).isTrue();
        assertThat(store2.matches("test/whatever")).isFalse();

        addSubs2(store2);
        store2.removeSubscriptions("client1");
        assertThat(store2.matches("sport/blubb/players1")).isFalse();
        assertThat(store2.matches("test/whatever")).isTrue();
        assertThat(store1.matches("sport/blubb/players1")).isTrue();
        assertThat(store1.matches("test/whatever")).isFalse();
    }
}
