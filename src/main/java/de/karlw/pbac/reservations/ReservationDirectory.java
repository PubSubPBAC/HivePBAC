package de.karlw.pbac.reservations;

import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;

import java.util.Collection;
import java.util.List;

public interface ReservationDirectory {
    public PurposeSet getSpecificAipForTopic(String topic);
    public void addReservation(Reservation reservation);
    public PurposeSet getAipForTopic(String topic);
    public boolean isTopicAllowed(String topic, Purpose ap);
    public Collection<Reservation> getAllReservations();
    public void logTree();
    public void clearAll();
    public PurposeSet getCombinedAIPForTopic(String topic);
    public PurposeSet getAffectedAIPForTopic(String topic);
}
