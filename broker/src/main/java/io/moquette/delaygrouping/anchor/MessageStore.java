package io.moquette.delaygrouping.anchor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStore {
    private Map<ByteBuffer, Integer> hashToCount = new ConcurrentHashMap<>();

    public void saveMessage(String topic, String payload) {
        save(topic + payload);
    }

    public void save(String value) {
        hashToCount.compute(getDigest(value), (key, oldValue) -> {
            if (oldValue == null) {
                return 1;
            } else {
                return oldValue + 1;
            }
        });
    }

    public boolean remove(String value) {
        var result = hashToCount.computeIfPresent(getDigest(value), (key, oldValue) -> {
            if (oldValue == 0) {
                return null;
            } else {
                return oldValue - 1;
            }
        });
        return result != null;
    }

    private ByteBuffer getDigest(String value) {
        try {
            return ByteBuffer.wrap(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }
}
