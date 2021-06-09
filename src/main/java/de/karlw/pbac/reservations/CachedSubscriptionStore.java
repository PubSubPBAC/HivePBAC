package de.karlw.pbac.reservations;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class CachedSubscriptionStore implements SubscriptionAPDirectory {

    SubscriptionAPDirectory subscriptionAPDirectory;
    HashMap<String, Boolean> cachedMatchingSubscriptionExists;
    
    private static final @NotNull Logger log = LoggerFactory.getLogger(CachedSubscriptionStore.class);

    public CachedSubscriptionStore(SubscriptionAPDirectory subscriptionAPDirectory) {
        this.subscriptionAPDirectory = subscriptionAPDirectory;
        cachedMatchingSubscriptionExists = new HashMap<>();
    }

    @Override
    public void updateSubscription(SubscriptionAP sap) {
        clearCache();
        subscriptionAPDirectory.updateSubscription(sap);
    }

    @Override
    public void removeSubscription(String clientId, String topic) {
        clearCache();
        subscriptionAPDirectory.removeSubscription(clientId, topic);
    }

    @Override
    public Purpose subscriptionPurpose(String clientId, String topic) {
        return subscriptionAPDirectory.subscriptionPurpose(clientId, topic);
    }

    @Override
    public SubscriptionAP getSubscription(String clientId, String topic) {
        return subscriptionAPDirectory.getSubscription(clientId, topic);
    }

    @Override
    public boolean matchingSubscriptionExists(String client, String topic, PurposeSet aip) {
        String cacheKey = client + "__" + topic + "__" + aip.toString();
        Boolean mse = cachedMatchingSubscriptionExists.get(cacheKey);

        // todo: null is a perfectly valid cachable result, maybe track misses otherwise?
        if (mse == null) {
            log.debug("sub cache MISS: {}", cacheKey);

            mse = subscriptionAPDirectory.matchingSubscriptionExists(client, topic, aip);
            cachedMatchingSubscriptionExists.put(cacheKey, mse);
        } else {
            log.debug("sub cache HIT: {}", cacheKey);
        }

        return mse;
    }

    @Override
    public List<SubscriptionAP> getTopicSubscriptions(String topic, boolean includeAffected) {
        return subscriptionAPDirectory.getTopicSubscriptions(topic, includeAffected);
    }

    @Override
    public List<SubscriptionAP> getAllSAPs() {
        return subscriptionAPDirectory.getAllSAPs();
    }

    @Override
    public void clearAll() {
        subscriptionAPDirectory.clearAll();
    }

    public void clearCache() {
        log.debug("sub cache: CLEAR");
        cachedMatchingSubscriptionExists = new HashMap<>();
    }
}
