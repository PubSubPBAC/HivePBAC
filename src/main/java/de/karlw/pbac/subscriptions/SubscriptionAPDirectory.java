package de.karlw.pbac.subscriptions;

import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;

import java.util.List;

public interface SubscriptionAPDirectory {
    void updateSubscription(SubscriptionAP sap);

    void removeSubscription(String clientId, String topic);

    Purpose subscriptionPurpose(String clientId, String topic);

    SubscriptionAP getSubscription(String clientId, String topic);

    boolean matchingSubscriptionExists(String client, String topic, PurposeSet aip);

    List<SubscriptionAP> getTopicSubscriptions(String topic, boolean includeAffected);
    List<SubscriptionAP> getAllSAPs();

    void clearAll();
}
