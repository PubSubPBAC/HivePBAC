package de.karlw.pbac;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.general.Qos;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.general.IterationCallback;
import com.hivemq.extension.sdk.api.services.general.IterationContext;
import com.hivemq.extension.sdk.api.services.publish.Publish;
import com.hivemq.extension.sdk.api.services.subscription.SubscriptionStore;
import com.hivemq.extension.sdk.api.services.subscription.SubscriptionsForClientResult;
import com.hivemq.extension.sdk.api.services.subscription.TopicSubscription;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.reservations.*;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;

public class CommandHandler {

    private static final @NotNull Logger log = LoggerFactory.getLogger(CommandHandler.class);

    public boolean setSetting(String key, String valueString) {
        PurposeSettings settings = PurposeSettings.getInstance();

        boolean value;

        if (valueString.equals("true")) {
            value = true;
        } else if (valueString.equals("false")) {
            value = false;
        } else {
            log.warn("non-boolean settings value {} for {}", valueString, key);
            return false;
        }

        settings.setSetting(key, value);
        log.info("setting {} to {}", key, value);
        return true;
    }

    public void logTree() {
        log.debug("logging tree:");
        ReservationDirectory reservationDirectory = PurposeManager.getInstance().getReservationDirectory();
        reservationDirectory.logTree();
    }

    public void persist() {
        log.debug("persisting because of command");
        PurposeManager.getInstance().persistenceHandler().persist();
    }

    public void reload() {
        log.debug("reloading because of command");
        PurposeManager.getInstance().persistenceHandler().load();
    }
    public void sendCombinedReservation(String topic) {

        ReservationDirectory reservationDirectory = PurposeManager.getInstance().getReservationDirectory();

        PurposeSet aip = reservationDirectory.getCombinedAIPForTopic(topic);
        String msgString = aip == null ? "none" : aip.toString();
        Publish message = Builders.publish()
                .topic(PurposeTopic.SETTING + "/CRES/" + PurposeTopic.encode(topic))
                .qos(Qos.AT_LEAST_ONCE)
                .payload(ByteBuffer.wrap(msgString.getBytes(StandardCharsets.UTF_8)))
                .build();

        Services.publishService().publish(message);
    }

    public void sendAffectedReservation(String topic) {
        ReservationDirectory reservationDirectory = PurposeManager.getInstance().getReservationDirectory();

        PurposeSet aip = reservationDirectory.getAffectedAIPForTopic(topic);
        String msgString = aip == null ? "none" : aip.toString();
        Publish message = Builders.publish()
                .topic(PurposeTopic.SETTING + "/ARES/" + PurposeTopic.encode(topic))
                .qos(Qos.AT_LEAST_ONCE)
                .payload(ByteBuffer.wrap(msgString.getBytes(StandardCharsets.UTF_8)))
                .build();

        Services.publishService().publish(message);
    }

    public void clear() {
        log.warn("clearing everything...");

        ReservationDirectory reservationDirectory = PurposeManager.getInstance().getReservationDirectory();
        SubscriptionAPDirectory subscriptionAPDirectory = PurposeManager.getInstance().getSubscriptionAPDirectory();

        subscriptionAPDirectory.clearAll();
        reservationDirectory.clearAll();

        SubscriptionStore subscriptionStore = Services.subscriptionStore();
        subscriptionStore.iterateAllSubscriptions(
                new IterationCallback<SubscriptionsForClientResult>() {
                    @Override
                    public void iterate(IterationContext context, SubscriptionsForClientResult subscriptionsForClient) {
                        // this callback is called for every client with its subscriptions
                        final String clientId = subscriptionsForClient.getClientId();
                        if (Arrays.asList(PurposeTopic.AAA_CLIENTS).contains(clientId)) {
                            log.debug("not clearing subscription for whitelisted client {}", clientId);
                            return;
                        }
                        final Set<TopicSubscription> subscriptions = subscriptionsForClient.getSubscriptions();
                        subscriptions.forEach((sub) -> {
                            subscriptionStore.removeSubscription(clientId, sub.getTopicFilter());
                        });
                    }
                });

    }

    public void reset() {
        log.warn("Resetting!");
        clear();
        PurposeManager.getInstance().reset();
    }
}
