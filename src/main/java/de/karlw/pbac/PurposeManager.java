package de.karlw.pbac;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.client.ClientContext;
import com.hivemq.extension.sdk.api.client.parameter.InitializerInput;
import com.hivemq.extension.sdk.api.services.Services;
import de.karlw.pbac.audit.AuditRecorder;
import de.karlw.pbac.audit.LogAuditRecorder;
import de.karlw.pbac.auth.APSubscriptionAuthorizer;
import de.karlw.pbac.auth.PBACAuthProvider;
import de.karlw.pbac.auth.PBACAuthorizerProvider;
import de.karlw.pbac.interceptors.*;
import de.karlw.pbac.persistence.CsvPurposePersistenceHandler;
import de.karlw.pbac.persistence.PurposePersistenceHandler;
import de.karlw.pbac.purpose_metadata.PmTree;
import de.karlw.pbac.reservations.*;
import de.karlw.pbac.subscriptions.SubscriptionAPDirectory;
import de.karlw.pbac.reservations.TreeBasedReservationDirectory;
import de.karlw.pbac.subscriptions.TreeBasedSubscriptionAPDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PersistenceDelegate;

public class PurposeManager {

    private static final @NotNull Logger log = LoggerFactory.getLogger(PurposeManager.class);
    private static final PurposeManager instance = new PurposeManager();

    private ReservationDirectory reservationDirectory;
    private SubscriptionAPDirectory subscriptionAPDirectory;
    private AuditRecorder auditRecorder;
    private PurposePersistenceHandler persistenceHandler;

    private CommandHandler commandHandler;

    private PbacMetrics metrics;

    synchronized public static PurposeManager getInstance() {
        return instance;
    }

    private PurposeManager() {
        log.debug("creating purpose manager");
        commandHandler = new CommandHandler();
        PbacMetrics metrics = new PbacMetrics();
        persistenceHandler = new CsvPurposePersistenceHandler();
        registerPurposeClasses();
    }

    public ReservationDirectory getReservationDirectory() {
        return reservationDirectory;
    }

    public SubscriptionAPDirectory getSubscriptionAPDirectory() {
        return subscriptionAPDirectory;
    }

    public PurposePersistenceHandler persistenceHandler() {
        return persistenceHandler;
    }

    public CommandHandler getCommandHandler() {
        return commandHandler;
    }

    public AuditRecorder getAuditRecorder() {
        return auditRecorder;
    }

    public void reset() {
        registerPurposeClasses();
        configureHive();
    }


    private void configureHive() {

        Services.initializerRegistry().setClientInitializer(
                this::initializer
        );

        PurposeManager.getInstance();
        log.debug("setting authorizer provider");

        if (PurposeSettings.get("filter_on_subscribe") || PurposeSettings.get("filter_hybrid")) {
            APSubscriptionAuthorizer apSubscriptionAuthorizer = new APSubscriptionAuthorizer();
            PBACAuthorizerProvider pbacAuthorizerProvider = new PBACAuthorizerProvider(apSubscriptionAuthorizer);
            Services.securityRegistry().setAuthorizerProvider(pbacAuthorizerProvider);
        } else {
            PBACAuthorizerProvider nullProvider = new PBACAuthorizerProvider(null);
            Services.securityRegistry().setAuthorizerProvider(nullProvider);
        }

        Services.securityRegistry().setAuthenticatorProvider(new PBACAuthProvider());

    }

    public void prepareForStop() {
        log.info("stopping, will persist purpose information...");
        persistenceHandler.persist();
    }

    private void initializer(InitializerInput initializerInput, ClientContext clientContext) {

        log.debug("initializing client {}", initializerInput.getClientInformation().getClientId());

        // check incoming messages for commands and reservations
        CommandInterceptor commandInterceptor = new CommandInterceptor(commandHandler);
        clientContext.addPublishInboundInterceptor(commandInterceptor);

        // extract and save AP from incoming subscriptions
        if (
                true // disabling this causes the server to not handle purpose-aware subscriptions
                    // in NoF mode
//                PurposeSettings.get("filter_on_subscribe") ||
//                        PurposeSettings.get("filter_on_publish") ||
//                        PurposeSettings.get("filter_hybrid")
        ) {

            InboundSubscriptionAPInterceptor apInterceptor = new InboundSubscriptionAPInterceptor();
            clientContext.addSubscribeInboundInterceptor(apInterceptor);
            InboundUnsubscribeAPInterceptor unsubscribeAPInterceptor = new InboundUnsubscribeAPInterceptor();
            clientContext.addUnsubscribeInboundInterceptor(unsubscribeAPInterceptor);
        }

        // filter on subscribe relies on an authorizer
//        if (PurposeSettings.get("filter_on_subscribe") || PurposeSettings.get("filter_hybrid")) {
//        }

        if (PurposeSettings.get("filter_on_publish") || PurposeSettings.get("filter_hybrid")) {
            CheckOutboundPublishInterceptor checkOutboundPublishInterceptor = new CheckOutboundPublishInterceptor();
            clientContext.addPublishOutboundInterceptor(checkOutboundPublishInterceptor);
        }


    }

    private void registerPurposeClasses() {
        try {

            if (PurposeSettings.getInstance().getSetting("use_tree_store", false)) {
                log.info("creating purpose classes with tree store");
                PmTree pmTree = new PmTree();
                this.subscriptionAPDirectory = new TreeBasedSubscriptionAPDirectory(pmTree);
                this.reservationDirectory = new TreeBasedReservationDirectory(pmTree);
            } else {
                log.info("creating purpose classes with flat store");
                this.subscriptionAPDirectory = new NaiveSubscriptionAPDirectory();
                this.reservationDirectory = new NaiveReservationDirectory();
            }

            // add a layer of caching if appropriate
            if (PurposeSettings.get("cache_reservations")) {
                ReservationDirectory cachableReservationDirectory = this.reservationDirectory;
                this.reservationDirectory = new CachedReservationStore(cachableReservationDirectory);
            }

            if (PurposeSettings.get("cache_subscriptions")) {
                SubscriptionAPDirectory cachableSubscriptionDirectory = this.subscriptionAPDirectory;
                this.subscriptionAPDirectory = new CachedSubscriptionStore(cachableSubscriptionDirectory);
            }

            auditRecorder = new LogAuditRecorder();

        } catch (Exception e) {
            log.error("Exception thrown creating PurposeManager: ", e);
        }
    }
}
