package de.karlw.pbac.reservations;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.general.Qos;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.subscription.TopicSubscription;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeCombiner;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NaiveReservationDirectory implements ReservationDirectory {

    private static final @NotNull Logger log = LoggerFactory.getLogger(NaiveReservationDirectory.class);
    private static Counter reservationsCounter = null;

    public enum AllowMode {
        ANY, ALL, DIRECT, BROADEST, SPECIFIC
    }


    // create the new metric, that is increased when a client isn't successfully authenticated
    Map<String, Reservation> reservations;

    public NaiveReservationDirectory() {

        this.reservations = new HashMap<>();

        log.debug("creating reservation directory");

        final MetricRegistry metricRegistry = Services.metricRegistry();
        reservationsCounter = metricRegistry.counter("de.karlw.pbac.reservations");
        reservationsCounter.dec(reservationsCounter.getCount()); // reset

    }

    public void addReservation(Reservation reservation) {
        String key = reservation.topic;

//        log.debug("adding reservation in {}", hashCode());

        if (!reservation.hasPurposes()) {
            log.debug("removing reservation for {}", key);
            if (this.reservations.remove(key) != null) {
                reservationsCounter.dec();
            }

        } else {
            log.debug("updating reservation for {}: {}", key, reservation.aip.toString());
            if (null == this.reservations.put(key, reservation)) {
                reservationsCounter.inc();
            }

        }

        updateSubscriptionsForReservation(reservation);
    }

    public void clearTopic(String topic) {
        this.reservations.remove(topic);
    }

    public Collection<Reservation> getAllReservations() {
        return reservations.values();
    }

    public Reservation getReservation(String topic) {
        Reservation r = this.reservations.get(topic);
//        log.debug("checking for reservation: {}, found {}", topic, r);
        return r;
    }

    public PurposeSet getSpecificAipForTopic(String topic) {
        Reservation r = getReservation(topic);
        if (r != null) {
            return r.aip;
        } else {
            return null;
        }
    }

    public boolean isTopicAllowed(String topic, Purpose ap) {

        PurposeSet aip = getAipForTopic(topic);

        if (aip == null || aip.isEmpty()) {
            log.debug("no reservations for {}", topic);
//            Services.metricRegistry().counter("de.karlw.pbac.allowed_no_ap").inc();

            return PurposeSettings.getInstance().getSetting("allow_without_reservation", true);
        } else {
            if (aip.allowsPurpose(ap)) {
                log.debug("allowing {} for {}, allowed are {}", topic, ap, aip.toString());
//                Services.metricRegistry().counter("de.karlw.pbac.allowed_ap").inc();
                return true;
            } else {
                Services.metricRegistry().counter("de.karlw.pbac.forbidden").inc();
//                log.debug("forbidding {} for {}, allowed are {}", topic, ap, aip.toString());
                return false;
            }
        }
    }

    @Override
    public void logTree() {
        log.debug(reservationOverview());
    }

    /**
     * this finds specific reservations that might get bypassed
     * by a wildcard subscription (affected)
     *
     * @param topic
     * @return
     */
    public List<Reservation> getAffectedReservations(String topic) {
        List<Reservation> matchingReservations = new ArrayList<>();

        log.debug("affected reservations for {}:", topic);
        reservations.forEach((rTopic, reservation) -> {
            if (PurposeTopic.topicMatches(topic, rTopic) && !topic.equals(rTopic)) {
                matchingReservations.add(reservation);
                log.debug(" - {}", reservation.toString());
            }
        });

        return matchingReservations;
    }

    /**
     * combined
     * @param topic
     * @return
     */
    public List<Reservation> getCombinedReservations(String topic) {

//        String[] nodes = topic.split("/");
        List<Reservation> matchingReservations = new ArrayList<>();

        // this method is more efficient, but would require a lot of iterations
        // when looking for PLUS reservations. could be improved in the future,
        // however the tree store is faster anyway

//        String nodeBuilt = "";
//        for (String nodeI : nodes) {
//            String wildcardTopicI = nodeBuilt + "#";
//            Reservation nodeIReservation = this.getReservation(wildcardTopicI);
//            if (nodeIReservation != null) {
//                matchingReservations.add(nodeIReservation);
//            }
//
//            // node is appended at the END, which is intended:
//            // example: my/great/topic
//            // - include #
//            // - don't include my/great/topic/#
//            nodeBuilt += nodeI + "/";
//        }
//
//        // include exact reservation at the end, because
//        Reservation exactReservation = this.getReservation(topic);
//        if (exactReservation != null) {
//            log.debug("found exact reservation ({})", topic);
//            matchingReservations.add(exactReservation);
//        }

        reservations.forEach((rTopic, reservation) -> {
//            log.debug("{} filtered by {} ?", rTopic, topic);
            if (PurposeTopic.topicMatches(rTopic, topic)) {
//                log.debug("YES!");
                matchingReservations.add(reservation);
            }
        });


        log.debug("found {} matching broader reservations for {}", matchingReservations.size(), topic);
        matchingReservations.forEach((reservation -> {
            log.debug(" - {}", reservation.toString());
        }));
        return matchingReservations;

    }

    public PurposeSet getAipForTopic(String topic) {
        // todo: combine more freely
        PurposeCombiner aipc = new PurposeCombiner();
        aipc.combine(getCombinedAIPForTopic(topic));
        aipc.combine(getAffectedAIPForTopic(topic), true);
        return aipc.getAip();
    }

    public String reservationOverview() {
        String r = "";
        for (Reservation reservation : reservations.values()) {
            r += reservation.topic + " - " + reservation.aip.toString() + "\n";
        }

        return r;
    }

    public static PurposeSet combineReservationAIP(List<Reservation> reservations, boolean restrict) {
        PurposeCombiner aipc = new PurposeCombiner();
        reservations.forEach((reservation) -> { aipc.combine(reservation.aip, restrict); });
        return aipc.getAip();
    }

    /**
     * depending on settings, pause subscriptions or flag them as hybrid
     * @param reservation
     */
    private void updateSubscriptionsForReservation(Reservation reservation) {

        if (!PurposeSettings.get("filter_on_subscribe") && !PurposeSettings.get("filter_hybrid")) {
            return;
        }

        boolean includeAffected = PurposeSettings.get("filter_on_subscribe");

        SubscriptionAPDirectory subscriptionAPs = PurposeManager.getInstance().getSubscriptionAPDirectory();
        subscriptionAPs.getTopicSubscriptions(reservation.topic, includeAffected).forEach((sub) -> {
            log.debug("rechecking {}", sub);

            PurposeSet aip = getAipForTopic(sub.topic);

            if (Arrays.asList(PurposeTopic.AAA_CLIENTS).contains(sub.clientId)) {
                log.debug("keeping subscription for whitelisted client");
            } else if (aip == null || aip.allowsPurpose(sub.ap)) {
                log.debug("compatible -- will add or keep");
                TopicSubscription ts = Builders.topicSubscription()
                        .topicFilter(sub.topic)
                        .qos(Qos.valueOf(sub.qos))
                        .build();

                Services.subscriptionStore().addSubscription(sub.clientId, ts);
            } else {
                log.debug("aip {} is incompatible with ap {}", aip, sub.ap);
                log.info("pausing subscription {} for {}", sub.topic, sub.clientId);
                Services.subscriptionStore().removeSubscription(sub.clientId, sub.topic);
            }
        });

    }

    public void clearAll() {
        log.warn("clearing all reservations");
        reservationsCounter.dec(reservations.size());
        this.reservations = new HashMap<>();
    }

    @Override
    public PurposeSet getCombinedAIPForTopic(String topic) {
        return combineReservationAIP(getCombinedReservations(topic), false); // todo
    }

    @Override
    public PurposeSet getAffectedAIPForTopic(String topic) {
        return combineReservationAIP(getAffectedReservations(topic), true);
    }


}
