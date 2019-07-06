package io.moquette.delaygrouping.anchor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStore {
    private Map<String, Integer> hashToCount = new ConcurrentHashMap<>();

    public void saveMessage(String topic, String payload) {
        save(topic + payload);
    }

    public void save(String value) {
        hashToCount.compute(value, (key, oldValue) -> {
            if (oldValue == null) {
                return 1;
            } else {
                return oldValue + 1;
            }
        });
    }

    public boolean remove(String value) {
        var result = hashToCount.computeIfPresent(value, (key, oldValue) -> {
            if (oldValue == 0) {
                return null;
            } else {
                return oldValue - 1;
            }
        });
        return result != null;
    }
}
