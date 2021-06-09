package de.karlw.pbac.purpose;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class PurposeSet {

    protected List<Purpose> aip;
    protected List<Purpose> pip;

    private static final @NotNull Logger log = LoggerFactory.getLogger(PurposeSet.class);

    public PurposeSet(String purposesString) {
        aip = new ArrayList<>();
        pip = new ArrayList<>();

        //        log.debug("purposes string: '{}'", purposesString);

//        log.debug(".purpoess.{}..", purposesString);

        if (purposesString.equals("") || purposesString.equals(PurposeTopic.PIP_SEP)) {
            return;
        }

        String[] aipPip = purposesString.split(Pattern.quote(PurposeTopic.PIP_SEP));
        String[] purposeStrings = aipPip[0].split(Pattern.quote(PurposeTopic.PURPOSE_SEP));

        for (String purposeString : purposeStrings) {
            if (!Objects.equals(purposeString, "")) {
                Purpose purpose = new Purpose(purposeString);
//                log.debug("adding purpose to set: {}", purpose.purposeString);
                aip.add(purpose);
            }
        }

        if (aipPip.length > 1) {
//            log.debug("forbidden: {}", aipPip[1]);

            purposeStrings = aipPip[1].split(Pattern.quote(PurposeTopic.PURPOSE_SEP));
            for (String purposeString : purposeStrings) {
                if (!Objects.equals(purposeString, "")) {
                    Purpose purpose = new Purpose(purposeString);
//                log.debug("adding purpose to set: {}", purpose.purposeString);
                    log.debug(purpose.purposeString);
                    pip.add(purpose);
                }
            }
        }

    }


    @Override
    public PurposeSet clone() {
//        super.clone();
        return new PurposeSet(toString());
    }

    /**
     * only keep AIP allowed by both sets
     * no changes when restricted with an empty set
     *
     * @param set PurposeSet to combine with
     */
    public void restrictWithSet(PurposeSet set) {

        if (set == null) {
            return;
        }

        ArrayList<Purpose> newAIP = new ArrayList<>();

        for (Purpose ap : this.aip) {
            if (ap.isCompatibleWithSet(set)) {
                newAIP.add(ap);
            }
        }

        // if purposes are nested, the other way round could still work
        for (Purpose ap : set.aip) {
            if (ap.isCompatibleWithSet(this) && !newAIP.contains(ap)) {
                newAIP.add(ap);
            }
        }

        this.aip = newAIP;
    }

    public void combineWithSet(PurposeSet set) {
        if (set == null) return;

        combinePurposes(aip, set.aip);
        combinePurposes(pip, set.pip);
    }

    private void combinePurposes(List<Purpose> p1, List<Purpose> p2) {
        p2.forEach((p) -> {
            p.addIfBroader(p1);
        });

    }


    public PurposeSet copy() {
        return new PurposeSet(toString());
    }

    public boolean allowsPurpose(Purpose ap) {
        for (Purpose aip : this.aip) {
//            log.debug(aip.purposeString);
            if (aip.isCompatibleWithPurpose(ap)) {
                for (Purpose pip : this.pip) {
                    if (pip.isCompatibleWithPurpose(ap))
                        return false;
                }
                return true;
            }
        }

        return false;
    }

    public String toString() {
        StringBuilder r = new StringBuilder();
        for (Purpose purpose : aip) {
            r.append(purpose.purposeString).append(",");
        }
        if (!pip.isEmpty()) {
            r.append(PurposeTopic.PIP_SEP);
            for (Purpose purpose : pip) {
                r.append(purpose.purposeString).append(",");
            }
        }

        return r.toString();
    }

    public boolean isEmpty() {
        return aip.size() == 0 && pip.size() == 0;
    }
}
