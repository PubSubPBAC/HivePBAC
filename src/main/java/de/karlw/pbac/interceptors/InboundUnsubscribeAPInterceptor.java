package de.karlw.pbac.interceptors;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.unsubscribe.UnsubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.unsubscribe.parameter.UnsubscribeInboundInput;
import com.hivemq.extension.sdk.api.interceptor.unsubscribe.parameter.UnsubscribeInboundOutput;
import com.hivemq.extension.sdk.api.packets.unsubscribe.ModifiableUnsubscribePacket;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundUnsubscribeAPInterceptor implements UnsubscribeInboundInterceptor {

    private static final Logger log = LoggerFactory.getLogger(InboundUnsubscribeAPInterceptor.class);

    @Override
    public void onInboundUnsubscribe(
            final @NotNull UnsubscribeInboundInput unsubscribeInboundInput,
            final @NotNull UnsubscribeInboundOutput unsubscribeInboundOutput
    ) {

        PurposeManager pm = PurposeManager.getInstance();
        SubscriptionAPDirectory subscriptions = pm.getSubscriptionAPDirectory();

        //get the modifiable disconnect object from the output
        final ModifiableUnsubscribePacket unsubscribePacket = unsubscribeInboundOutput.getUnsubscribePacket();
        // modify / overwrite parameters of the disconnect packet.
        try {
            for (String topic : unsubscribePacket.getTopicFilters()) {
                // remove from store
                String clientId = unsubscribeInboundInput.getClientInformation().getClientId();
                log.debug("removing subscription {} for {}", topic, clientId);
                subscriptions.removeSubscription(clientId, topic);
            }
        } catch (final Exception e) {
            log.debug("Unsubscribe inbound interception failed:", e);
        }
    }
}