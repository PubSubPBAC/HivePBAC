package de.karlw.pbac.audit;

import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.subscriptions.SubscriptionAP;

public interface AuditRecorder {

    public void onSubscribe(SubscriptionAP sap);
    public void onReserve(String clientId, String topic, PurposeSet aip);
    public void onPublish(String topic, String clientId);


}
