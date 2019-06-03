package io.moquette.bridge;

import java.util.concurrent.ConcurrentHashMap;

public class IdStore {
    private final ConcurrentHashMap.KeySetView<String, Boolean> STORE = ConcurrentHashMap.newKeySet();

    public void add(String id) {
        STORE.add(id);
    }

    public Boolean contains(String id) {
        return STORE.contains(id);
    }
}
