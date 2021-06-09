package de.karlw.pbac.purpose_metadata;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeCombiner;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.reservations.Reservation;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PmNode {

    private static final @NotNull Logger log = LoggerFactory.getLogger(PmNode.class);

    //    private final String nodeName;
    private final HashMap<String, PmNode> subNodes;
    private final HashMap<String, SubscriptionAP> subscriptions;
    private PurposeSet reservation;
    private final TopicString originalTopic;

    public Reservation getReservationObject() {
        return new Reservation(originalTopic.toString(), getReservation());
    }

    public PmNode() {
        this(TopicString.emptyString());
    }

    public PmNode(TopicString originalTopic) {
        subNodes = new HashMap<>();
        subscriptions = new HashMap<>();
        this.originalTopic = originalTopic;
    }

    public void logTree(int level) {
        subNodes.forEach((name, node) -> {
            log.debug("-".repeat(level) + " " + name + ": " + node.toString());
            node.logTree(level + 1);
        });
    }

    public PmNode getSubnode(TopicString topicString, boolean create) {
        PmNode subNode = subNodes.get(topicString.next().toString());

        if (subNode == null) {
            if (create) {
                subNode = addNewSubnode(topicString.next().toString());
            } else {
                return null;
            }
        }

        if (topicString.tail() == null) {
            return subNode;
        } else {
            return subNode.getSubnode(topicString.tail(), create);
        }

    }

    public List<PmNode> getSubnodes(TopicString topicString) {
        List<PmNode> nodes = new ArrayList<>();
        if (topicString.next().isHash()) {
            subNodes.forEach((name, node) -> {
                nodes.add(node);
                nodes.addAll(node.getSubnodes(TopicString.hashString()));
            });
        } else if (topicString.next().isPlus()) {
            subNodes.forEach((name, node) -> {
                nodes.add(node);
                nodes.addAll(node.getSubnodes(topicString.tail()));
            });
        } else { // single subnode
            PmNode subNode = subNodes.get(topicString.next().toString());
            if (subNode != null) {
                nodes.add(subNode);
                if (topicString.tail() != null) {
                    nodes.addAll(subNode.getSubnodes(topicString.tail()));
                }
            }
        }

        return nodes;
    }

//    public PurposeSet getAffectedAIP(TopicString topicString) {
//        log.debug("checking for affected in {}", (topicString == null ? null : topicString.next()));
//        if (topicString == null || topicString.isEmpty()) {
//            // we've arrived in the exactly right topic
//            return getAIP();
//        } else if (topicString.next().isPlus() || topicString.next().isHash()) {
//            PurposeCombiner combinedAIP = new PurposeCombiner();
//            subNodes.forEach((name, node) -> {
//                log.debug("calling {} with...", name);
//                combinedAIP.combine(node.getAffectedAIP(topicString.tail()));
//            });
//            return combinedAIP.getAip();
//        } else {
//            // go down in the tree, return matching subnode's AIP or null
//            log.debug("checking for affected in {}", topicString.next());
//            PmNode subnode = getSubnode(topicString.next(), false);
//            return subnode != null ? subnode.getAffectedAIP(topicString.tail()) : null;
//        }
//    }

    /**
     * First, find the right topic, then resolve the wildcard
     *
     * @return
     */
    public PurposeSet getAffectedAIP(TopicString topicString) {
        if (topicString == null || topicString.isEmpty()) {
            return getAllSubAIP();
        }

        if (topicString.next().isHash()) {
            return getAllSubAIP();
        } else if (topicString.next().isPlus()) {
            PurposeCombiner aip = new PurposeCombiner();
            subNodes.forEach((name, node) -> {
                log.debug("calling plus-subnode {}", name);
                aip.combine(node.getAffectedAIP(topicString.tail()));
            });
            return aip.getAip();
        } else {
            PmNode subnode = getSubnode(topicString.next(), false);
            return subnode == null ? null : subnode.getAffectedAIP(topicString.tail());
        }
    }


    public PurposeSet getAllSubAIP() {
        PurposeCombiner aip = new PurposeCombiner();
        aip.combine(getAIP());
        subNodes.forEach((name, node) -> {
            aip.combine(node.getAllSubAIP());
        });
        return aip.getAip();
    }

    public PurposeSet getCombinedAIP(TopicString topicString) {

        if (topicString == null || topicString.isEmpty()) {
            return getAIP();
        }

        PurposeCombiner aip = new PurposeCombiner();
        TopicString n = topicString.next();

        /**
         * for a plus node, use all subnodes
         * else use plus, hash + direct
         */
        if (n != null && n.isPlus()) {
            subNodes.forEach((name, node) -> {
                aip.merge(node.getCombinedAIP(topicString.tail()));
            });
            return aip.getAip();
        }

        if (n != null && !n.isWildcardTopic() && !n.isEmpty()) {
            PmNode directNode = getSubnode(n, false);
            if (directNode != null) {
//                log.debug("checking combined for {}", n);
                aip.merge(directNode.getCombinedAIP(topicString.tail()));
            }
        }

        PmNode hashNode = getSubnode(TopicString.hashString(), false);
        if (hashNode != null) {
//            log.debug("checking combined for hash");
            aip.merge(hashNode.getCombinedAIP(TopicString.emptyString()));
        }

        PmNode plusNode = getSubnode(TopicString.plusString(), false);
        if (plusNode != null) {
//            log.debug("checking combined for plus");
            aip.merge(plusNode.getCombinedAIP(topicString.tail()));
        }

        return aip.getAip();
    }


    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("res: ");
        s.append(reservation);
        s.append(" subs: ");
        s.append(subscriptions.size());
        return s.toString();
    }

    public PmNode addNewSubnode(String nodeName) {
        PmNode subNode = subNodes.get(nodeName);
        if (subNode != null) {
            log.warn("tried recreating existing subnode {}", nodeName);
        } else {
            subNode = new PmNode(originalTopic.subtopic(nodeName));
            subNodes.put(nodeName, subNode);
        }

        return subNode;
    }

    /**
     * check subscriptions that are AFFECTED, meaning that they are
     * PARTLY concerned by a reservation and may or may not be
     * forbidden by it.
     * <p>
     * This only applies to wildcard subscriptions.
     * <p>
     * Strategy: walk down the tree until matching the topic,
     * check wildcard subscriptions on the way
     *
     * @param topicString
     */
    public List<SubscriptionAP> getAffectedSubscriptions(TopicString topicString, boolean matched) {
        List<SubscriptionAP> subs = new ArrayList<>();
        log.debug(" searching affected: {} remaining ({})", topicString, matched);

        if (topicString == null || topicString.isEmpty()) {
            if (matched) {
                subs.addAll(getSubscriptionAPs());
            }
        } else {

            if (topicString.next().isToken()) {
                PmNode subnode = getSubnode(topicString.next(), false);
                if (subnode != null) {
                    subs.addAll(subnode.getAffectedSubscriptions(topicString.tail(), false));
                }
            }

            PmNode hashNode = getSubnode(TopicString.hashString(), false);
            if (hashNode != null) {
                subs.addAll(hashNode.getAffectedSubscriptions(TopicString.emptyString(), true));
            }

            PmNode plusNode = getSubnode(TopicString.plusString(), false);
            if (plusNode != null) {
                subs.addAll(plusNode.getAffectedSubscriptions(topicString.tail(), true));
            }

        }

        return subs;
    }


    /**
     * <p>
     * When entering the node, consider the following cases:
     * <p>
     * - empty: we have reached an exact match, check subscriptions
     * - some token: we have not reached, continue with next token
     * - plus: continue with all subtokens, including the plus
     * - hash: match ALL subnodes, but not this one
     *
     * @param topicString
     */
    public List<SubscriptionAP> getSpecifiedSubscriptions(TopicString topicString) {

        List<SubscriptionAP> subs = new ArrayList<>();

//        log.debug(" checking specified for {}", topicString);
        if (topicString == null || topicString.isEmpty()) {
            subs.addAll(getSubscriptionAPs());
        } else if (topicString.isHash()) {
            subs.addAll(getSubscriptionAPs());
            subNodes.forEach((name, node) -> {
                subs.addAll(node.getSpecifiedSubscriptions(TopicString.hashString()));
            });
        } else if (topicString.next().isPlus()) {
            subNodes.forEach((name, node) -> {
                subs.addAll(node.getSpecifiedSubscriptions(topicString.tail()));
            });
        } else { // direct match
//            log.debug("direct match {}", topicString.next());
            PmNode subnode = getSubnode(topicString.next(), false);
            if (subnode != null) {
                subs.addAll(subnode.getSpecifiedSubscriptions(topicString.tail()));
            }
        }

        return subs;
    }

    public List<SubscriptionAP> getSubscriptionAPs() {
        return new ArrayList<>(subscriptions.values());
    }

    public void setSubscription(String clientId, SubscriptionAP sap) {
        log.debug("setting subscription: {} with {}", clientId, sap.ap);
        this.subscriptions.put(clientId, sap);
    }

    public void removeSubscription(String clientId) {
        this.subscriptions.remove(clientId);
    }

    public HashMap<String, SubscriptionAP> getSubscriptions() {
        return subscriptions;
    }

    public Purpose getAPForClient(String clientId) {
        SubscriptionAP sap = subscriptions.get(clientId);
        if (sap == null) {
            return null;
        } else {
            return sap.ap;
        }
    }

    public List<SubscriptionAP> getSubscriptionAPsRecursively() {
        List<SubscriptionAP> saps = this.getSubscriptionAPs();
        this.subNodes.forEach((name, node) -> {
            saps.addAll(node.getSubscriptionAPsRecursively());
        });
//        log.debug("found {} saps", saps.size());
        return saps;
    }

    public List<Reservation> getReservationsRecursively() {
        List<Reservation> reservations = new ArrayList<>();
        Reservation currentNodeReservation = getReservationObject();
        if (currentNodeReservation != null) {
            reservations.add(currentNodeReservation);
        }
        this.subNodes.forEach((name, node) -> {
            reservations.addAll(node.getReservationsRecursively());
        });
        return reservations;
    }

    public SubscriptionAP getClientSubscription(String clientId) {
        return subscriptions.get(clientId);
    }

    public PurposeSet getReservation() {
        return reservation;
    }

    public void setReservation(PurposeSet reservation) {
        this.reservation = reservation;
    }

    public PurposeSet getAIP() {
        return getReservation();
    }

}
