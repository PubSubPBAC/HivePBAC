package de.karlw.pbac.purpose;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PurposeTopic {

    public static final String RESERVE = "!RESERVE";
    public static final String AIP = "!AIP";
    public static final String AP = "!AP";
    public static final String PURPOSE_BEGIN = "{";
    public static final String PURPOSE_END = "}";
    public static final String PURPOSE_SEP = ",";
    public static final String PIP_SEP = "|";
    public static final String HASH = "HASH";
    public static final String PLUS = "PLUS";
    public static final String SETTING = "!PBAC";

    public static final String[] AAA_CLIENTS = new String[]{"mqtt-explorer-1"};

    private static final @NotNull Logger log = LoggerFactory.getLogger(PurposeTopic.class);

    public Purpose ap;
    public String topic;

    public PurposeTopic(String topic, Purpose ap) {
        this.ap = ap;
        this.topic = topic;
    }

    public String toString() {
        return topic + " AP: " + ap.toString();
    }

    public static PurposeTopic fromTopicPart(String apTopicPart) {
        return fromTopic(PurposeTopic.AP + "/" + apTopicPart);
    }

    public static PurposeTopic fromTopic(String apTopic) {
        Pattern purposeTopicPattern = Pattern.compile("(?:" + PurposeTopic.AP + ")/(.*)\\{(.*)}"); // todo: use constants
        Matcher matcher = purposeTopicPattern.matcher(apTopic);
        if (matcher.find()) {
            String topic = matcher.group(1);
            Purpose ap = new Purpose(matcher.group(2));
            PurposeTopic pt = new PurposeTopic(topic, ap);
            log.debug("parsed purpose topic {} for {}", topic, ap.toString());
            return pt;
        } else {
            log.warn("not matched AP for {}", apTopic);
            return null;
        }
    }


    public static String encode(String topic) {
        return topic.replace("#", HASH).replace("+", PLUS);
    }

    public static boolean topicMatches(String filter, String topic) {
        if (topic.equals(filter)) {
            return true;
        } else if (filter.contains("+")) {
            // if the topic to be matched contains plusses, make sure we match
            // it anyway
            boolean withPlus = topic.contains("+");
            return topic.matches(makeRegex(filter, withPlus));
        } else if (filter.endsWith("#")) { // should be faster than regex
            String matcher = filter.substring(0, filter.length() - 1);
            return topic.startsWith(matcher);
        } else return false;
    }

    public static String decode(String topic) {
        return topic.replace(HASH, "#").replace(PLUS, "+");
    }

    public static String makeRegex(String filter, boolean withPlus) {
        if (withPlus) {
                // temporarily save plusses in the original filter
                filter = filter
                        .replace("+", "++")
                        .replaceAll("([^/#+]+)", "($1|\\\\+)")
                        .replace("#", "*")
                        .replace("++", "[^/]*")
                // todo: empty nodes
                ;
                log.debug("DOUBLE PATTERN: {}", filter);
            } else {
                filter = filter
                    .replace("+", "[^/]*")
                    .replace("#", ".*");
            }
            return filter;
    }

}
