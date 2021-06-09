package de.karlw.pbac.purpose_metadata;

public class TopicString {
    private final String topic;
    private String previous;

    public TopicString(String topic) {
        this.topic = topic;
    }

    public TopicString(String topic, String previous) {
        this.topic = topic;
        this.previous = previous;
    }

    public String toString() {
        return topic;
    }

    public boolean multilevel() {
        return topic.contains("/");
    }

    public TopicString next() {
        String[] split = topic.split("/", 2);
        return new TopicString(split[0]);
    }

    public boolean hasNext() {
        return (next() != null && !next().isEmpty());
    }



    public TopicString tail() {
        if (!multilevel()) {
            return null;
        } else {
            String[] split = topic.split("/", 2);
            return new TopicString(split[1], split[0]);
        }
    }

    public boolean isPlus() {
        return topic.equals("+");
    }

    public TopicString subtopic(String name) {
        if (isEmpty()) {
            return new TopicString(name);
        } else {
            return new TopicString(topic + "/" + name);
        }
    }

    public boolean isHash() {
        return topic.equals("#");
    }

    public boolean isToken() {
        return !(isEmpty() || isPlus() || isHash());
    }

    public boolean isEmpty() {
        return topic.equals("");
    }

    public static TopicString hashString() {
        return new TopicString("#");
    }

    public static TopicString plusString() {
        return new TopicString("+");
    }

    public static TopicString emptyString() {
        return new TopicString("");
    }

    public boolean isWildcardTopic() {
        return topic.contains("#") || topic.contains("+");
    }
}
