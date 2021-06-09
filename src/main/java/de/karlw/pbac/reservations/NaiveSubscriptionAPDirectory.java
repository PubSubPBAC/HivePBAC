package de.karlw.pbac.reservations;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NaiveSubscriptionAPDirectory implements SubscriptionAPDirectory {

    // hierarchy: client_id --> topic --> access Purpose

    private static final @NotNull Logger log = LoggerFactory.getLogger(NaiveSubscriptionAPDirectory.class);


    public Map<String, Map<String, SubscriptionAP>> subscriptions;

    public NaiveSubscriptionAPDirectory() {
        this.subscriptions = new HashMap<>();
    }

    private Map<String, SubscriptionAP> getClientMap(String clientId) {
        return subscriptions.getOrDefault(clientId, new HashMap<>());
    }

    @Override
    public void updateSubscription(SubscriptionAP sap) {
        Map<String, SubscriptionAP> clientMap = getClientMap(sap.clientId);
        clientMap.put(sap.topic, sap);
        subscriptions.put(sap.clientId, clientMap);
    }

    @Override
    public void removeSubscription(String clientId, String topic) {
        Map<String, SubscriptionAP> clientMap = getClientMap(clientId);
        clientMap.remove(topic);
    }

    @Override
    public Purpose subscriptionPurpose(String clientId, String topic) {

        Purpose apOrNull = null;
        Map<String, SubscriptionAP> clientSubscriptions = getClientMap(clientId);

        SubscriptionAP sapOrNull = clientSubscriptions.get(topic);

        return apOrNull;
    }

    @Override
    public SubscriptionAP getSubscription(String clientId, String topic) {
        Map<String, SubscriptionAP> clientSubscriptions = getClientMap(clientId);
        return clientSubscriptions.get(topic);
    }

    @Override
    public boolean matchingSubscriptionExists(String client, String topic, PurposeSet aip) {
        boolean found = false;

        for (Map.Entry<String, SubscriptionAP> e : getClientMap(client).entrySet()) {
            if (PurposeTopic.topicMatches(e.getKey(), topic)) {
                Purpose ap = sapAp(e.getValue());
                log.debug("matching topic: {} for {}, ap: {}", e.getKey(), topic, ap);
                if (ap.isCompatibleWithSet(aip)) {
                    found = true;
                    break;
                }
            }
        }

        return found;
    }

    public void clearAll() {
        log.warn("clearing all subscriptions");
        this.subscriptions = new HashMap<>();
    }

    /**
     * @param topic
     * @return
     */
    public List<SubscriptionAP> getTopicSubscriptions(String topic, boolean includeAffected) {
        List<SubscriptionAP> subscriptionAPs = new ArrayList<>();

        subscriptions.forEach((clientId, clientSubscriptionAPs) -> {
            clientSubscriptionAPs.forEach((sTopic, sub) -> {
                if (PurposeTopic.topicMatches(sTopic, topic)) {
                    subscriptionAPs.add(sub);
                    log.debug("reservation changed for {}: subscription {} is relevant ({})",
                            topic, sTopic, clientId);
                } else if (includeAffected && PurposeTopic.topicMatches(topic, sTopic)) {
                    // affected
                    subscriptionAPs.add(sub);
                    log.debug("reservation changed for {}: subscription {} is affected ({})",
                            topic, sTopic, clientId);
                }
            });
        });


        return subscriptionAPs;
    }

    @Override
    public List<SubscriptionAP> getAllSAPs() {
        List<SubscriptionAP> all = new ArrayList<>();
        subscriptions.forEach((x, clientSubscriptionMap) -> {
            all.addAll(clientSubscriptionMap.values());
        });
        return all;
    }

    private Purpose sapAp(SubscriptionAP sap) {
        if (sap == null) {
            return null;
        } else {
            return sap.ap;
        }
    }
}
