package de.karlw.pbac.subscriptions;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose_metadata.PmNode;
import de.karlw.pbac.purpose_metadata.PmTree;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TreeBasedSubscriptionAPDirectory implements SubscriptionAPDirectory {

    private static final @NotNull Logger log =
            LoggerFactory.getLogger(TreeBasedSubscriptionAPDirectory.class);
    private final PmTree pmTree;

    public TreeBasedSubscriptionAPDirectory(PmTree pmTree) {
        this.pmTree = pmTree;
    }

    @Override
    public void updateSubscription(SubscriptionAP sap) {
        PmNode node = pmTree.getNode(sap.topic, true);
        node.setSubscription(sap.clientId, sap);
    }

    @Override
    public void removeSubscription(String clientId, String topic) {
//        updateSubscription(clientId, topic, null);
                PmNode node = pmTree.getNode(topic, true);
                node.removeSubscription(clientId);
    }

    @Override
    public Purpose subscriptionPurpose(String clientId, String topic) {
        PmNode node = pmTree.getNode(topic, false);
        if (node != null) {
            return node.getAPForClient(clientId);
        } else {
            return null;
        }

    }

    @Override
    public SubscriptionAP getSubscription(String clientId, String topic) {
        PmNode node = pmTree.getNode(topic, false);
        if (node != null) {
            return node.getClientSubscription(clientId);
        } else return null;
    }

    @Override
    public boolean matchingSubscriptionExists(String client, String topic, PurposeSet aip) {
       return pmTree.matchingSubscriptionExists(client, topic, aip);
    }

    @Override
    public List<SubscriptionAP> getTopicSubscriptions(String topic, boolean includeAffected) {
        // todo: not needed
        PmNode node = pmTree.getNode(topic, false);
        if (node != null) {
            return node.getSubscriptionAPs();
        } else return new ArrayList<SubscriptionAP>();
    }

    public List<SubscriptionAP> getAllSAPs() {
        return pmTree.getAllSubscriptionAPs();
    }

    @Override
    public void clearAll() {
        this.pmTree.reset();
    }
}
