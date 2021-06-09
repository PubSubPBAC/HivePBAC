package de.karlw.pbac;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PurposeSettings {

    private static final Logger LOG = LoggerFactory.getLogger(PurposeSettings.class);
    private static PurposeSettings instance;

    Map<String, Boolean> settings;

    private PurposeSettings() {
        settings = new HashMap<>();
        addDefault();
    }

    public static PurposeSettings getInstance() {
        if (PurposeSettings.instance == null) {
            instance = new PurposeSettings();
        }

        return instance;
    }

    public static boolean get(String key) {
        return getInstance().getSetting(key, false);
    }

    private void addDefault() {
        settings.put("allow_without_reservation", true);
        settings.put("filter_on_subscribe", true);
        settings.put("filter_on_publish", false);
        settings.put("filter_hybrid", false);
        settings.put("hybrid_tag", false);
        settings.put("use_tree_store", false);
        settings.put("cache_reservations", true);
        settings.put("cache_subscriptions", true);
        settings.put("auto_persist", false);
    }

    public boolean getSetting(String key, boolean fallback) {
        warnIfNew(key);
        return settings.getOrDefault(key, fallback);
    }

    public void setSetting(String key, boolean value) {
        warnIfNew(key);
        settings.put(key, value);
    }

    public void warnIfNew(String key) {
        if (!settings.containsKey(key)) {
            LOG.warn("using non-existing key {}", key);
        }

    }

    public Map<String, Boolean> getSettings() {
        return settings;
    }


}
