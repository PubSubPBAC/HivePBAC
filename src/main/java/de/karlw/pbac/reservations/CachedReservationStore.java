package de.karlw.pbac.reservations;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;

public class CachedReservationStore implements ReservationDirectory {
    HashMap<String, PurposeSet> cachedCombinedReservations;
    ReservationDirectory reservationDirectory;
    
    private static final @NotNull Logger log = LoggerFactory.getLogger(CachedReservationStore.class);

    public CachedReservationStore(ReservationDirectory reservationDirectory) {
        this.reservationDirectory = reservationDirectory;
        cachedCombinedReservations = new HashMap<>();
    }

    @Override
    public PurposeSet getSpecificAipForTopic(String topic) {
        return reservationDirectory.getSpecificAipForTopic(topic);
    }

    @Override
    public void addReservation(Reservation reservation) {
        clearCache();
        reservationDirectory.addReservation(reservation);
    }

    @Override
    public PurposeSet getAipForTopic(String topic) {
        return reservationDirectory.getAipForTopic(topic);
    }

    @Override
    public boolean isTopicAllowed(String topic, Purpose ap) {
        return reservationDirectory.isTopicAllowed(topic, ap);
    }

    @Override
    public Collection<Reservation> getAllReservations() {
        return reservationDirectory.getAllReservations();
    }

    @Override
    public void logTree() {
        reservationDirectory.logTree();
    }

    @Override
    public void clearAll() {
        reservationDirectory.clearAll();
        clearCache();
    }

    @Override
    public PurposeSet getCombinedAIPForTopic(String topic) {
        PurposeSet combined = cachedCombinedReservations.get(topic);

        if (combined == null) {
            log.debug("Reservation Cache: MISS for {}", topic);
            combined = reservationDirectory.getCombinedAIPForTopic(topic);
            cachedCombinedReservations.put(topic, combined);
        }
        else {
            log.debug("Reservation Cache: HIT for {}", topic);
        }

        return combined;

    }

    @Override
    public PurposeSet getAffectedAIPForTopic(String topic) {
        return reservationDirectory.getAffectedAIPForTopic(topic);
    }

    public void clearCache() {

        log.debug("Reservation Cache: CLEAR");
        cachedCombinedReservations = new HashMap<>();
    }
}
