
/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.karlw.pbac.interceptors;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import de.karlw.pbac.CommandHandler;
import de.karlw.pbac.PurposeManager;
import de.karlw.pbac.PurposeSettings;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeTopic;
import de.karlw.pbac.reservations.Reservation;
import de.karlw.pbac.reservations.ReservationDirectory;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class CommandInterceptor implements PublishInboundInterceptor {

    private static final @NotNull Logger log = LoggerFactory.getLogger(CommandInterceptor.class);

    CommandHandler commandHandler;

    public CommandInterceptor(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
        log.debug("command interceptor initialized");
    }

    @Override
    public void onInboundPublish(
            final @NotNull PublishInboundInput publishInboundInput,
            final @NotNull PublishInboundOutput publishInboundOutput
    ) {
        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();

//        log.debug("inbound packet id {}", publishPacket.getPacketId());

        String topic = publishPacket.getTopic();

//        log.debug("PUB topic: {}", topic);


        // fail fast
        if (topic.charAt(0) != '!') {
            return;
        }

        PurposeManager pm = PurposeManager.getInstance();


        if (topic.startsWith(PurposeTopic.RESERVE)) {
            ReservationDirectory reservations = pm.getReservationDirectory();
            Reservation reservation = Reservation.fromTopic(topic);
            if (reservation != null) {
                String clientId = publishInboundInput.getClientInformation().getClientId();
                PurposeManager.getInstance().getAuditRecorder().onReserve(clientId, reservation.topic, reservation.aip);
                reservations.addReservation(reservation);
                if (PurposeSettings.get("auto_persist")) {
                    pm.persistenceHandler().persistReservations();
                }
//                log.debug(reservations.reservationOverview());
            }

            return;
        }


        if (topic.startsWith(PurposeTopic.SETTING)) {

            String presubTrigger = PurposeTopic.SETTING + "/PRESUB/";
            // todo: presub qos
            if (topic.startsWith(presubTrigger)) {
                String clientIdAndTopicString = topic.substring(presubTrigger.length());
                String[] clientIdAndTopic = clientIdAndTopicString.split("/", 2);
                if (clientIdAndTopic.length < 2) return;
                String clientId = clientIdAndTopic[0];
                PurposeTopic pt = PurposeTopic.fromTopicPart(clientIdAndTopic[1]);
                log.debug("will add presub {} for {}", pt.toString(), clientId);
                SubscriptionAP sap = new SubscriptionAP(clientId, pt.topic, pt.ap, 1);
                PurposeManager.getInstance().getSubscriptionAPDirectory().updateSubscription(sap);
            }

            if (topic.equals(PurposeTopic.SETTING + "/CLEAR")) {
                commandHandler.clear();
                return;
            }

            if (topic.equals(PurposeTopic.SETTING + "/PERSIST")) {
                commandHandler.persist();
                return;
            }

            if (topic.equals(PurposeTopic.SETTING + "/RELOAD")) {
                commandHandler.reload();
                return;
            }

            if (topic.equals(PurposeTopic.SETTING + "/RESET")) {
                log.debug("received RESET packet with id {}, qos {}",
                        publishPacket.getPacketId(),
                        publishPacket.getQos()
                );
                commandHandler.reset();
                return;
            }

            if (topic.equals(PurposeTopic.SETTING + "/TREE")) {
                commandHandler.logTree();
                return;
            }

            if (topic.startsWith(PurposeTopic.SETTING + "/SET/")) {
                String key = topic.substring(topic.lastIndexOf('/') + 1);
                //noinspection OptionalGetWithoutIsPresent
                String value = StandardCharsets.UTF_8.decode(
                        publishPacket.getPayload().get()
                ).toString();
                commandHandler.setSetting(key, value);
            }

            if (topic.startsWith(PurposeTopic.SETTING + "/CRES/")) {
                topic = PurposeTopic.decode(topic);
                String key = topic.split("/CRES/")[1];
                log.debug("publishing CRES for {}", key);
                commandHandler.sendCombinedReservation(key);
//                publishInboundOutput.preventPublishDelivery();
            }

            if (topic.startsWith(PurposeTopic.SETTING + "/ARES/")) {
                topic = PurposeTopic.decode(topic);
                String key = topic.split("/ARES/")[1];
                log.debug("publishing ARES for {}", key);
                commandHandler.sendAffectedReservation(key);
//                publishInboundOutput.preventPublishDelivery();
            }
        }
    }

}