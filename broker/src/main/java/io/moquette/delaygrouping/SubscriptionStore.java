package io.moquette.delaygrouping;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionStore implements Serializable {
    private Set<String> subscriptions = ConcurrentHashMap.newKeySet();
    private Map<String, Set<String>> subsPerClient = new ConcurrentHashMap<>();

    public SubscriptionStore(SubscriptionStore objToCopy) {
        objToCopy.subsPerClient.forEach((clientId, topicList) -> {
            topicList.forEach(topic -> this.addSubscription(clientId, topic));
        });
    }

    public SubscriptionStore() {
    }

    public boolean matches(String topic) {
        for (String sub : subscriptions) {
            if (Utils.mqttTopicMatchesSubscription(topic, sub)) {
                return true;
            }
        }
        return false;
    }

    public void addSubscription(String clientId, String topicFilter) {
        var subList = subsPerClient.computeIfAbsent(clientId, k -> new HashSet<>());
        subList.add(topicFilter);

        subscriptions.add(topicFilter);
    }

    public void removeSubscriptions(String clientId) {
        var subs = subsPerClient.remove(clientId);
        // If we actually removed smth, rebuild subscription list
        if (subs != null) {
            subscriptions.clear();
            subsPerClient.forEach((id, subList) -> {
                subscriptions.addAll(subList);
            });
        }
    }

    public void merge(SubscriptionStore store) {
        store.subsPerClient.forEach((clientId, topicList) -> {
            topicList.forEach(topic -> this.addSubscription(clientId, topic));
        });
    }

    public Set<String> getFlattened() {
        return new HashSet<>(subscriptions);
    }

    public void clear() {
        subscriptions.clear();
        subsPerClient.clear();
    }

    @Override
    public String toString() {
        return subscriptions.toString();
    }
}
