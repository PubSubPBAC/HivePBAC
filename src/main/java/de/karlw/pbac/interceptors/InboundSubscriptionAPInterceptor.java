package de.karlw.pbac.interceptors;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.subscribe.SubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundInput;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundOutput;
import com.hivemq.extension.sdk.api.packets.subscribe.ModifiableSubscribePacket;
import com.hivemq.extension.sdk.api.packets.subscribe.ModifiableSubscription;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.audit.AuditRecorder;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundSubscriptionAPInterceptor implements SubscribeInboundInterceptor {

    private static final @NotNull Logger log = LoggerFactory.getLogger(InboundSubscriptionAPInterceptor.class);

    /**
     * Read and remove AIP from the topic string and save them into a user property instead.
     * We can then authorize the subscription later based on the user property
     *
     * @param subscribeInboundInput
     * @param subscribeInboundOutput
     */

    @Override
    public void onInboundSubscribe(
            final @NotNull SubscribeInboundInput subscribeInboundInput,
            final @NotNull SubscribeInboundOutput subscribeInboundOutput) {

        // get the modifiable subscribe packet from the output
        final ModifiableSubscribePacket subscribe = subscribeInboundOutput.getSubscribePacket();

        PurposeManager pm = PurposeManager.getInstance();
        SubscriptionAPDirectory subscriptions = pm.getSubscriptionAPDirectory();

        for (ModifiableSubscription subscription : subscribe.getSubscriptions()) {
            String topic = subscription.getTopicFilter();

            log.debug("intercepting subscription {}, sub class: {}", topic, subscriptions.getClass().getName());
            if (topic.startsWith(PurposeTopic.AP)) {
                topic = PurposeTopic.decode(topic);
                PurposeTopic pt = PurposeTopic.fromTopic(topic);
                topic = pt.topic;
                String clientId = subscribeInboundInput.getClientInformation().getClientId();
                SubscriptionAP sap = new SubscriptionAP(clientId, pt.topic, pt.ap, subscription.getQos().getQosNumber());
                log.debug("subscription: {}, saving", sap);
                AuditRecorder audit = PurposeManager.getInstance().getAuditRecorder();
                audit.onSubscribe(sap);
                subscriptions.updateSubscription(sap);
                if (PurposeSettings.get("auto_persist")) {
                    pm.persistenceHandler().persistSubscriptionAPs();
                }

                // for hive, set a non-purpose topic as topic
                subscription.setTopicFilter(topic);
            } else {
                log.debug("topic without ap");
//                String clientId = subscribeInboundInput.getClientInformation().getClientId();
//                subscriptions.updateSubscription(clientId, topic, null);
            }

            // store topic data in store, even if without AP, so we can find it later


        }

        // modify / overwrite any parameter of the subscribe packet
//        final ModifiableSubscription subscription = subscribe.getSubscriptions().get(0);
//        subscription.setTopicFilter("modified-topic");
//        subscription.setQos(Qos.AT_LEAST_ONCE);
//        subscription.setRetainHandling(RetainHandling.DO_NOT_SEND);
//        subscription.setRetainAsPublished(true);
//        subscription.setNoLocal(true);
//        subscribe.getUserProperties().addUserProperty("additional", "value");
    }
}

