package de.karlw.pbac.persistence;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.reservations.Reservation;
import de.karlw.pbac.reservations.ReservationDirectory;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

abstract public class PurposePersistenceHandler {

    private static final @NotNull Logger log = LoggerFactory.getLogger(PurposePersistenceHandler.class);

    public void load() {
        List<SubscriptionAP> ss = loadSubscriptionAPs();
        SubscriptionAPDirectory sd = PurposeManager.getInstance().getSubscriptionAPDirectory();
        log.debug("loading {} saps from file", ss.size());
        ss.forEach(sd::updateSubscription);

        List<Reservation> rs = loadReservations();
        ReservationDirectory rd = PurposeManager.getInstance().getReservationDirectory();
        log.debug("loading {} reservations from file", rs.size());
        rs.forEach(rd::addReservation);
        rd.logTree();

    }

    public void persist() {
        persistReservations();
        persistSubscriptionAPs();
    }

    public void persistReservations() {
        PurposeManager pm = PurposeManager.getInstance();
        Collection<Reservation> reservations = pm.getReservationDirectory().getAllReservations();
        saveReservations(reservations);
    }

    public void persistSubscriptionAPs() {
        PurposeManager pm = PurposeManager.getInstance();
        SubscriptionAPDirectory sad = pm.getSubscriptionAPDirectory();
        Collection<SubscriptionAP> saps = sad.getAllSAPs();
        saveSubscriptions(saps);
    }

    abstract void saveSubscriptions(Collection<SubscriptionAP> saps);

    abstract List<SubscriptionAP> loadSubscriptionAPs();

    abstract void saveReservations(Collection<Reservation> reservations);

    abstract List<Reservation> loadReservations();


}
