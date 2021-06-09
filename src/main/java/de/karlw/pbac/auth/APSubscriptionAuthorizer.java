package de.karlw.pbac.auth;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.auth.SubscriptionAuthorizer;
import com.hivemq.extension.sdk.api.auth.parameter.SubscriptionAuthorizerInput;
import com.hivemq.extension.sdk.api.auth.parameter.SubscriptionAuthorizerOutput;
import com.hivemq.extension.sdk.api.services.Services;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.reservations.*;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class APSubscriptionAuthorizer implements SubscriptionAuthorizer {

    private static final @NotNull Logger log = LoggerFactory.getLogger(APSubscriptionAuthorizer.class);

    public APSubscriptionAuthorizer() {

    }

    @Override
    public void authorizeSubscribe(
            @NotNull final SubscriptionAuthorizerInput input,
            @NotNull final SubscriptionAuthorizerOutput output
    ) {

        // when not filtering on subscribe, allow all subscriptions
        if (
                !PurposeSettings.get("filter_on_subscribe") &&
                        !PurposeSettings.get("filter_hybrid")
        ) {
            log.warn("using subscribe authorizer redundantly!");
            output.authorizeSuccessfully();
            return;
        }

        String topic = input.getSubscription().getTopicFilter();
        String clientId = input.getClientInformation().getClientId();

        // allow whitelisted clients
        if (Arrays.asList(PurposeTopic.AAA_CLIENTS).contains(clientId)) {
            output.authorizeSuccessfully();
            log.debug("allowing subscription to {} for whitelisted client {}", topic, clientId);
            return;
        }

        PurposeManager pm = PurposeManager.getInstance();
        SubscriptionAPDirectory subscriptionDirectory = pm.getSubscriptionAPDirectory();
        ReservationDirectory reservationDirectory = pm.getReservationDirectory();

//        Purpose apOrNull = subscriptionDirectory.subscriptionPurpose(clientId, topic);
        SubscriptionAP subscriptionAP = subscriptionDirectory.getSubscription(clientId, topic);

        log.debug("found subscription: {}", subscriptionAP);
        log.debug("using reserveration directory {}", reservationDirectory.hashCode());

        PurposeSet aip;
        if (PurposeSettings.get("filter_hybrid")) {
            aip = reservationDirectory.getCombinedAIPForTopic(topic);
//                        aip = reservationDirectory.getAipForTopic(topic);
        } else {
            aip = reservationDirectory.getAipForTopic(topic);
        }

        // in Hybrid Tag mode, if the affected topics AIP do not allow the AP, we set the
        // subscription as affected, because SOME topics are not allowed. HT mode will then
        // re-check the actual message topic on publish.
//        if (subscriptionAP != null && PurposeSettings.get("hybrid_tag")) {
//            PurposeSet affectedAip = reservationDirectory.getAffectedAIPForTopic(topic);
//            if (affectedAip != null && !affectedAip.allowsPurpose(subscriptionAP.ap)) {
//                subscriptionAP.setAffected(true);
//            }
//        }

        Purpose apOrNull = null;
        if (subscriptionAP != null) apOrNull = subscriptionAP.ap;

        boolean allowSubscription;
        if (aip == null) {
            log.debug("handling subscription without reservation");
            allowSubscription = PurposeSettings.get("allow_without_reservation");
        } else {
            allowSubscription = aip.allowsPurpose(apOrNull);
            log.debug("checking aip for subscription: {} -> {}", aip, allowSubscription);
        }

        if (allowSubscription) {
            log.debug("allow");
            Services.metricRegistry().counter("de.karlw.pbac.allowed").inc();
            output.authorizeSuccessfully();
        } else {
            Services.metricRegistry().counter("de.karlw.pbac.forbidden").inc();
            output.failAuthorization();
        }

    }

}
