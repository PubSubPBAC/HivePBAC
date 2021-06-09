package de.karlw.pbac.interceptors;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishOutboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishOutboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishOutboundOutput;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.reservations.*;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class CheckOutboundPublishInterceptor implements PublishOutboundInterceptor {

    private static final @NotNull Logger log = LoggerFactory.getLogger(CheckOutboundPublishInterceptor.class);

    @Override
    public void onOutboundPublish(
            final @NotNull PublishOutboundInput input,
            final @NotNull PublishOutboundOutput output
    ) {
        final String clientId = input.getClientInformation().getClientId();
        final String topic = input.getPublishPacket().getTopic();

        // don't filter monitoring packets
        if (!topic.startsWith("$SYS/pbac")) {
//            log.debug("publishing packet to {} ({})", input.getPublishPacket().getTopic(), input.getClientInformation().getClientId());
             PurposeManager.getInstance().getAuditRecorder().onPublish(topic, clientId);
        }

        // don't filter if filter on publish is off
        // (although the whole interceptor should not be active in that case)
        if (
                !PurposeSettings.get("filter_on_publish") &&
                        !PurposeSettings.get("filter_hybrid")
        ) {
            // this doesn't work as expected with whitelisted clients staying connected through a reset
//            log.warn("outbound publish interceptor active in FoS / NoF");
            return;
        }

        // client whitelist
        if (Arrays.asList(PurposeTopic.AAA_CLIENTS).contains(clientId)) {
            return;
        }

        log.debug("filtering on publish...");

        PurposeManager pm = PurposeManager.getInstance();
        SubscriptionAPDirectory subscriptionDirectory = pm.getSubscriptionAPDirectory();
        ReservationDirectory reservationDirectory = pm.getReservationDirectory();

        PurposeSet aip = reservationDirectory.getCombinedAIPForTopic(topic);

        if (aip != null) {
            log.debug("finding matching subscription for aip {}:", aip);
            if (!subscriptionDirectory.matchingSubscriptionExists(clientId, topic, aip)) {
                log.debug("preventing publishing packet to {} for {}  (allowed: {})", topic, clientId, aip);
                output.preventPublishDelivery();
            }
        } else if (!PurposeSettings.get("allow_without_reservation")) {
            log.debug("forbidding message to unreserved topic");
            output.preventPublishDelivery();
        }


    }
}