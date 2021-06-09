
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

package de.karlw.pbac;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.client.ClientContext;
import com.hivemq.extension.sdk.api.client.parameter.InitializerInput;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import de.karlw.pbac.auth.APSubscriptionAuthorizer;
import de.karlw.pbac.auth.PBACAuthProvider;
import de.karlw.pbac.auth.PBACAuthorizerProvider;
import de.karlw.pbac.interceptors.*;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.reservations.*;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main class of the extension,
 * which is instantiated either during the HiveMQ start up process (if extension is enabled)
 * or when HiveMQ is already started by enabling the extension.
 *
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class HivePBACMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(HivePBACMain.class);

    boolean DEVELOPMENT = true;

    @Override
    public void extensionStart(
            final @NotNull ExtensionStartInput extensionStartInput,
            final @NotNull ExtensionStartOutput extensionStartOutput
    ) {

        PurposeManager.getInstance().reset();

        if (DEVELOPMENT) {
            testPersistence();
        } else {
            PurposeManager.getInstance().persistenceHandler().load();
        }

        final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
        log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

    }

    @Override
    public void extensionStop(
            final @NotNull ExtensionStopInput extensionStopInput,
            final @NotNull ExtensionStopOutput extensionStopOutput
    ) {
        PurposeManager.getInstance().prepareForStop();
        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

    }


    public void testPersistence() {
        PurposeManager pm = PurposeManager.getInstance();
        SubscriptionAPDirectory sd = pm.getSubscriptionAPDirectory();
        sd.updateSubscription(new SubscriptionAP(
                "test_client",
                "test_topic",
                new Purpose("testing"),
                1
        ));
        pm.getReservationDirectory().addReservation(new Reservation(
                "test/reservation",
                new PurposeSet("test/persistence")
        ));

        pm.persistenceHandler().persist();
        pm.getCommandHandler().clear();
        pm.persistenceHandler().load();
    }


}
