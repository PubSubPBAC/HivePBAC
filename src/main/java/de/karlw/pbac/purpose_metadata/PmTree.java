package de.karlw.pbac.purpose_metadata;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.general.Qos;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.general.IterationCallback;
import com.hivemq.extension.sdk.api.services.general.IterationContext;
import com.hivemq.extension.sdk.api.services.subscription.SubscriptionsForClientResult;
import com.hivemq.extension.sdk.api.services.subscription.TopicSubscription;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeCombiner;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.reservations.Reservation;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class PmTree {

    private static final @NotNull Logger log = LoggerFactory.getLogger(PmTree.class);

    private PmNode rootNode;

    public PmTree() {
        rootNode = new PmNode();
//        getNode("test/1/2/3", true);
//        getNode("test/1/2/dff", true);
//        getNode("test/1/2/3/4", true);
//        getNode("test/1/2/3/5", true);
//        getNode("test/1/2/asdf", true);
//        logTree();
//        log.debug("ex: {}", getNode("test/1/2/3", false));
//        log.debug("inex: {}", getNode("test/1/2/44", false));
    }

    public void reset() {
        rootNode = new PmNode();
    }

    public PmNode getNode(String topic, boolean create) {
        TopicString topicString = new TopicString(topic);
        return rootNode.getSubnode(topicString, create);
    }

    public void logTree() {
        rootNode.logTree(0);
    }

    public List<PmNode> getNodes(String topicString) {
        return rootNode.getSubnodes(new TopicString(topicString));
    }

    /**
     * Get the combination of all relevant reservation, which together
     * define the AIP of the target topic.
     * <p>
     * For example, for a reservation of patient/34/heart, check for:
     * - #
     * - patient/#
     * - patient/34/#
     * - +/+/+
     * - patient/+/#
     * - ...
     *
     * @param topic
     * @return
     */
    public PurposeSet getCombinedAIPForTopic(String topic) {
        PurposeSet combinedAIP = rootNode.getCombinedAIP(new TopicString(topic));
        log.debug("combined AIP for {}: {}", topic, combinedAIP);
        return combinedAIP;
    }

    /**
     * get the intersection of all reservation AIP from all possible subtopics
     *
     * @param topic
     * @return
     */
    public PurposeSet getAffectedAIPForTopic(String topic) {
        log.debug("-- getting affected for {}", topic);
        TopicString topicString = new TopicString(topic);
        if (!topicString.isWildcardTopic()) {
            log.debug("{} is not a wildcard topic, no affected reservations", topic);
            return null;
        }

        PurposeSet aip = rootNode.getAffectedAIP(topicString);
        log.debug("affected aip for {}: {}", topic, aip);
        return aip;

//        PurposeSet restrictedAIP = null;
//        List<PmNode> subNodes = rootNode.getSubnodes(topicString);
//        for (PmNode node: subNodes) {
//
//            if (restrictedAIP == null) {
//                restrictedAIP = node.getAIP() != null ? node.getAIP().copy() : null;
//            } else {
//                restrictedAIP.restrictWithSet(node.getAIP());
//            }
//        }
//
//        return restrictedAIP;
    }


    /**
     * After changing a reservation, check all subscriptions that are specified by
     * the wildcard reservation.
     * <p>
     * For a non-wildcard reservation, only the exact match is returned.
     * <p>
     * For a hash wildcard, all sub-nodes are checked for changed subscriptions
     * <p>
     * For a plus wildcard, all matching nodes are checked for changed subscriptions.
     * <p>
     * A reservation has changed, so we need to find out which subscription
     * are now potentially invalid. This concerns only subscriptions that are now
     * FULLY forbidden, i.e. there is no possible topic that could still be allowed.
     * <p>
     * For a changed reservation of patient/+/sensors/#, this means:
     * <p>
     * - patient/# is not relevant ❌ (e.g. patient/personal is not affected)
     * - patient/+/# is not relevant ❌
     * - patient/+/sensors is not relevant ❌
     * - patient/+/sensors/# is relevant ✅
     * - patient/2/sensors/# is relevant ✅
     * - patient/2/sensors/heart is relevant ✅
     * - patient/2/sensors/heart/# is relevant ✅
     * <p>
     * For a changed reservation of patient/+/sensors, this means:
     * <p>
     * - patient/# ❌
     * - patient/+/sensors is relevant ✅
     * - patient/2/sensors is relevant ✅
     * - patient/+/sensors/# is not relevant ❌
     *
     * @param topic
     */
    public void checkSpecifiedSubscriptions(String topic) {
        log.debug("checking specified...");

        TopicString topicString = new TopicString(topic);
        List<SubscriptionAP> subs = rootNode.getSpecifiedSubscriptions(topicString);

        subs.forEach(this::checkSubscription);
    }

    public boolean matchingSubscriptionExists(String client, String topic, PurposeSet aip) {
        if (aip == null) {
            return true;
        }

        // affected does not include the sub itself, so we'll check it beforehand:
        PmNode node = rootNode.getSubnode(new TopicString(topic), false);
        if (node != null) {
            HashMap<String, SubscriptionAP> directSubs = node.getSubscriptions();

            if (directSubs != null &&
                    directSubs.get(client) != null &&
                    directSubs.get(client).ap.isCompatibleWithSet(aip)) {
                log.debug("found direct match for {} ({}), allowing", topic, client);
                return true;
            }

        }


        /**
         * todo idea: drastically optimize performance by checking while going to the tree so we can
         * exit the algorithm on first match
         */

        List<SubscriptionAP> subs = rootNode.getAffectedSubscriptions(new TopicString(topic), false);
        for (SubscriptionAP sub : subs) {
            if (!sub.clientId.equals(client)) {
                continue;
            }
            log.debug("checking subscription comp of {}", sub);
            if (aip.allowsPurpose(sub.ap)) {
                log.debug("found compatible sub for {} ({}), allowing", topic, client);
                return true;
            }
        }

        log.debug("found neither direct or affected sub for {} ({}), forbidding", topic, client);
        return false;
    }


    /**
     * After a reservation has changed, check for AFFECTED subscriptions,
     * i.e. those that might now be PARTIALLY forbidden by a new AP.
     * <p>
     * This is only called if filter on publish is off
     *
     * @param topic
     */
    public void checkAffectedSubscriptions(String topic) {
        log.debug("checking affected...");
        TopicString topicString = new TopicString(topic);
        List<SubscriptionAP> subs = rootNode.getAffectedSubscriptions(topicString, false);

        subs.forEach(this::checkSubscription);
    }

    public void checkSubscription(SubscriptionAP sap) {

        log.debug("checking: {}", sap);

        if (Arrays.asList(PurposeTopic.AAA_CLIENTS).contains(sap.clientId)) {
            log.debug("keeping subscription for whitelisted client");
        } else if (isAllowed(sap)) {
            log.debug("compatible -- will add or keep");
            TopicSubscription ts = Builders.topicSubscription()
                    .topicFilter(sap.topic)
                    .qos(Qos.AT_MOST_ONCE) // todo: get actual qos
                    .build();

            Services.subscriptionStore().addSubscription(sap.clientId, ts);
        } else {
            log.info("pausing subscription {} for {}", sap.topic, sap.clientId);
            log.debug("{}", Services.metricRegistry().getCounters().get("com.hivemq.subscriptions.overall.current").getCount());

            try {
                Services.subscriptionStore().removeSubscription(sap.clientId, sap.topic).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            log.debug("remaining subscriptions: ");
            Services.subscriptionStore().iterateAllSubscriptions(new IterationCallback<SubscriptionsForClientResult>() {
                @Override
                public void iterate(IterationContext context, SubscriptionsForClientResult subscriptionsForClient) {
                    // this callback is called for every client with its subscriptions
                    final String clientId = subscriptionsForClient.getClientId();
                    final Set<TopicSubscription> subscriptions = subscriptionsForClient.getSubscriptions();
                    StringBuilder s = new StringBuilder();
                    s.append(clientId + ": ");
                    subscriptions.forEach((subscription) -> {
                        s.append(subscription.toString() + ", ");
                    });
                    log.debug(s.toString());
                }
            });

            log.debug("{}", Services.metricRegistry().getCounters().get("com.hivemq.subscriptions.overall.current").getCount());

        }
    }

    public List<SubscriptionAP> getAllSubscriptionAPs() {
        return rootNode.getSubscriptionAPsRecursively();
    }

    public List<Reservation> getAllReservations () {
        return rootNode.getReservationsRecursively();
    }

    public boolean isAllowed(SubscriptionAP sap) {
        return isAllowed(sap.topic, sap.ap);
    }

    public boolean isAllowed(String topic, Purpose ap) {
        PurposeCombiner aip = new PurposeCombiner();
        aip.combine(getCombinedAIPForTopic(topic));

        // todo: if blabla
        aip.combine(getAffectedAIPForTopic(topic));

        return aip.noneSet() || aip.getAip().allowsPurpose(ap);
    }


}
