package io.moquette.delaygrouping.anchor;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExpiringCacheTest {
    @Test
    public void testCache() throws InterruptedException {
        var instance = new ExpiringCache<String>(500);
        instance.add("Value1");
        Thread.sleep(200);
        instance.add("Value2");

        assertThat(instance.getAll()).containsExactlyInAnyOrder("Value1", "Value2");

        Thread.sleep(100);

        assertThat(instance.getAll()).containsExactlyInAnyOrder("Value1", "Value2");

        Thread.sleep(250);

        assertThat(instance.getAll()).containsExactlyInAnyOrder("Value2");

        Thread.sleep(200);

        assertThat(instance.getAll()).isEmpty();

    }

}
