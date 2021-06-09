package de.karlw.pbac.purpose;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PurposeCombiner {

    private PurposeSet aip = null;

    private static final @NotNull Logger log = LoggerFactory.getLogger(PurposeCombiner.class);

    public void combine(PurposeSet set) {
        combine(set, true);
    }

    public void merge(PurposeSet set) {
        combine(set, false);
    }

    public void combine(PurposeSet set, boolean restrict) {
//        log.debug("combining {} with {}", aip, set);
        if (aip == null) {
            aip = set != null ? set.clone() : null;
        } else {
            if (restrict)
                aip.restrictWithSet(set);
            else
                aip.combineWithSet(set);
        }
    }

    public boolean noneSet() {
        return aip == null;
    }

    public PurposeSet getAip() {
        return aip;
    }
}

