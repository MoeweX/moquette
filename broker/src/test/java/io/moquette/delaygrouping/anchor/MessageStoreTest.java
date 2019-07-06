package io.moquette.delaygrouping.anchor;


import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageStoreTest {

    @Test
    public void test() {
        var instance = new MessageStore();
        instance.save("msg1");
        instance.save("msg2");
        assertThat(instance.remove("msg1")).isTrue();
        assertThat(instance.remove("msg1")).isFalse();
        instance.save("msg2");
        assertThat(instance.remove("msg2")).isTrue();
        assertThat(instance.remove("msg2")).isTrue();
        assertThat(instance.remove("msg3")).isFalse();
        assertThat(instance.remove("msg2")).isFalse();
    }
}
