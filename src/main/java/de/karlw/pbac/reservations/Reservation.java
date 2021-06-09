package de.karlw.pbac.reservations;


import com.hivemq.extension.sdk.api.annotations.NotNull;
import de.karlw.pbac.purpose.Purpose;
import de.karlw.pbac.purpose.PurposeSet;
import de.karlw.pbac.purpose.PurposeTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Reservation {

    private static final @NotNull
    Logger log = LoggerFactory.getLogger(Reservation.class);


    public String topic;
    public PurposeSet aip;

    public Reservation(String topic, PurposeSet aip) {
        this.topic = topic;
        this.aip = aip;
    }

    public boolean hasPurposes() {
        return aip != null && !aip.isEmpty();
    }

    public boolean allowsTopic(Purpose ap) {
        return hasPurposes() && aip.allowsPurpose(ap);
    }

    public String toString() {
        String purposeString;
        if (hasPurposes()) {
            purposeString = aip.toString();
        } else {
            purposeString = "NULL";
        }

        return String.format("topic: %s, purposes: %s", topic, purposeString);
    }

    public static Reservation fromTopic(String topicWithPurpose) {
        topicWithPurpose = PurposeTopic.decode(topicWithPurpose);
        String pattern = String.format(
                "(?:%s|%s)/(.*)\\%s(.*)%s", // todo: determine if {} needs to be escaped
                PurposeTopic.RESERVE,
                PurposeTopic.AIP,
                PurposeTopic.PURPOSE_BEGIN,
                PurposeTopic.PURPOSE_END
                );

        Pattern purposeTopicPattern = Pattern.compile(pattern); // todo: move into PurposeTopic
        Matcher matcher = purposeTopicPattern.matcher(topicWithPurpose);
        if (matcher.find()) {
//            log.trace("matched!");
            String topic = matcher.group(1);
            PurposeSet aip = new PurposeSet(matcher.group(2));
            log.trace("aip: >{}<", aip.toString());
            Reservation reservation = new Reservation(topic, aip);

            return reservation;
        } else {
            // this should only be called after checking if it's a purpose topic
            // hence warn if it doesn't match
            log.warn("topic could not be matched");
            return null;
        }
    }
}
