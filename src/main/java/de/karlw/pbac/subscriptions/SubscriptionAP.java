package de.karlw.pbac.subscriptions;

import de.karlw.pbac.purpose.Purpose;

public class SubscriptionAP {

    public String clientId;
    public String topic;
    public Purpose ap;
    public int qos;
    public boolean affected;

    public SubscriptionAP(String clientId, String topic, Purpose ap, int qos) {
        this.clientId = clientId;
        this.topic = topic;
        this.ap = ap;
        this.qos = qos; // todo: real qos
    }

    public void setAffected(boolean affected) {
        this.affected = affected;
    }

    public boolean isAffected() {
        return affected;
    }

    public String toString() {
        return String.format("sub: %s for %s (%s)", topic, ap, clientId);
    }

//    public String[] toStringArray() {
//        String[] fields = {
//                clientId,
//                topic,
//                ap.toString(),
//                String.valueOf(qos),
//        };
//        return fields;
//    }
//
//    public static SubscriptionAP fromStringArray(String[] fields) {
//        if (fields.length < 4) {
//            return null;
//        }
//
//        String clientId = fields[0];
//        String topic = fields[1];
//        Purpose ap = new Purpose(fields[2]);
//        int qos = Integer.valueOf(fields[3]);
//        return new SubscriptionAP(clientId, topic, ap, qos);
//    }
}
