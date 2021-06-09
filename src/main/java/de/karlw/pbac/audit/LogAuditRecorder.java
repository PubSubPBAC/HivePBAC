package de.karlw.pbac.audit;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.subscriptions.SubscriptionAP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAuditRecorder implements AuditRecorder {

    private static final @NotNull Logger log = LoggerFactory.getLogger(LogAuditRecorder.class);

    @Override
    public void onSubscribe(SubscriptionAP sap) {
        log("subscribe: {}", sap);
    }

    @Override
    public void onReserve(String clientId, String topic, PurposeSet aip) {
        log("client {} reserving {} for {}", clientId, topic, aip);
    }

    @Override
    public void onPublish(String topic, String clientId) {
//        log("publish to {} for {}", topic, clientId);
    }

    private void log(String msg, Object ...o) {
        log.debug("[AUDIT] " + msg, o);
    }


}
