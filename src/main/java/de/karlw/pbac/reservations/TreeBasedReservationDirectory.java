package de.karlw.pbac.reservations;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.services.Services;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeCombiner;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose_metadata.PmNode;
import de.karlw.pbac.purpose_metadata.PmTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TreeBasedReservationDirectory implements ReservationDirectory {

    private static final @NotNull Logger log = LoggerFactory.getLogger(TreeBasedReservationDirectory.class);

    private final PmTree tree;
    private static Counter reservationsCounter = null;

    public TreeBasedReservationDirectory(PmTree tree) {
        this.tree = tree;
        final MetricRegistry metricRegistry = Services.metricRegistry();
        reservationsCounter = metricRegistry.counter("de.karlw.pbac.reservations");
        reservationsCounter.dec(reservationsCounter.getCount()); //reset

    }


    @Override
    public PurposeSet getSpecificAipForTopic(String topic) {
        PmNode node = tree.getNode(topic, false);
        if (node != null) {
            return node.getReservation();
        } else {
            return null;
        }
    }

    @Override
    public void addReservation(Reservation reservation) {
//        log.debug("adding reservation for {}: {}", reservation.topic, reservation.aip);
        PmNode node = tree.getNode(reservation.topic, true);
        boolean hadReservation = (node.getReservation() != null);

        if (!reservation.hasPurposes()) {
            log.debug("removing reservation for {}", reservation.topic);
            node.setReservation(null);
            if (hadReservation) {
                reservationsCounter.dec();
            }

        } else {
            log.debug("updating reservation for {}: {}", reservation.topic, reservation.aip.toString());
            node.setReservation(reservation.aip);
            if (!hadReservation) {
                reservationsCounter.inc();
            }
        }

        if (PurposeSettings.get("filter_on_subscribe") || PurposeSettings.get("filter_hybrid")) {
            log.debug("checking specified subscriptions....");
            tree.checkSpecifiedSubscriptions(reservation.topic);
        }

        if (PurposeSettings.get("filter_on_subscribe")) {
            log.debug("checking affected subscriptions....");
            tree.checkAffectedSubscriptions(reservation.topic);
        }
    }

    @Override
    public PurposeSet getAipForTopic(String topic) {
        PurposeCombiner aip = new PurposeCombiner();
        aip.combine(getCombinedAIPForTopic(topic));
        aip.combine(getAffectedAIPForTopic(topic));
        return aip.getAip();
    }

    @Override
    public boolean isTopicAllowed(String topic, Purpose ap) {

        PurposeSet aip = getAipForTopic(topic);

        if (aip == null || aip.isEmpty()) {
            log.debug("no reservations for {}", topic);
            Services.metricRegistry().counter("de.karlw.pbac.allowed_no_ap").inc();

            return PurposeSettings.getInstance().getSetting("allow_without_reservation", true);
        }

        boolean allowed = aip.allowsPurpose(ap);

        log.debug("is {} compatible with {}? {}", ap, topic, allowed);
        return allowed;
    }

    @Override
    public Collection<Reservation> getAllReservations() {
        return tree.getAllReservations();
    }


    @Override
    public void logTree() {
        tree.logTree();
    }

    @Override
    public void clearAll() {
        log.warn("resetting reservation tree, this will remove subscriptions as well");
        tree.reset();
    }



    @Override
    public PurposeSet getCombinedAIPForTopic(String topic) {
        return tree.getCombinedAIPForTopic(topic);
    }

    public PurposeSet getAffectedAIPForTopic(String topic) {
        return tree.getAffectedAIPForTopic(topic);
    }
}
