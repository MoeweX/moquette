package io.moquette.delaygrouping;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionStore {
    private Set<String> subscriptions = ConcurrentHashMap.newKeySet();
    private Map<String, Set<String>> subsPerClient = new ConcurrentHashMap<>();

    public boolean matches(String topic) {
        for (String sub : subscriptions) {
            if (Utils.mqttTopicMatchesSubscription(topic, sub)) {
                return true;
            }
        }
        return false;
    }

    public void addSubscription(String clientId, String topicFilter) {
        var subList = subsPerClient.get(clientId);
        if (subList == null) {
            subList = new HashSet<>();
            subsPerClient.put(clientId, subList);
        }
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

    public Set<String> getFlattened() {
        return new HashSet<>(subscriptions);
    }

    public void clear() {
        subscriptions.clear();
        subsPerClient.clear();
    }
}
