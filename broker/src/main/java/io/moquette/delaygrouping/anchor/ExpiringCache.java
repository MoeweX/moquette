package io.moquette.delaygrouping.anchor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ExpiringCache<T> {
    private long expirationTime;
    private Map<T, Long> entries = new ConcurrentHashMap<>();

    public ExpiringCache(int expirationTime) {
        this.expirationTime = expirationTime * 1_000_000L;
    }

    public void add(T value) {
        entries.put(value, System.nanoTime());
    }

    public Set<T> getAll() {
        var currentTime = System.nanoTime();
        var expiredEntries = entries.entrySet().stream()
            .filter(entry -> currentTime - entry.getValue() > expirationTime)
            .collect(Collectors.toSet());

        // Remove values only if timestamp has not been updated in the meantime
        expiredEntries.forEach(entry -> entries.remove(entry.getKey(), entry.getValue()));

        return new HashSet<>(entries.keySet());
    }

    public void clear() {
        entries.clear();
    }
}
