package de.karlw.pbac.purpose;

import java.util.List;

public class Purpose {

    public String purposeString;

    public Purpose(String purposeString) {
        this.purposeString = purposeString;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Purpose)) {
            return false;
        }

        return ((Purpose) o).toString().equals(this.toString());
    }

    public boolean isCompatibleWithSet(PurposeSet set) {
        if (set != null) {
            return set.allowsPurpose(this);
        } else {
            return false;
        }
    }

    /**
     * research allows ap research
     * research allows ap research/marketing
     *
     * @param ap
     * @return
     */
    boolean isCompatibleWithPurpose(Purpose ap) {
        if (ap == null) return false; // no access ever without a purpose

        return ap.purposeString.equals(this.purposeString) ||
                ap.purposeString.startsWith(this.purposeString + "/");
    }

    public boolean isBroaderThan(Purpose cp) {
        return (purposeString.length() < cp.purposeString.length());
    }

    Purpose moreSpecificPurpose(Purpose cp) {
        if (!isCompatibleWithPurpose(cp)) {
            return null;
        } else if (cp.purposeString.length() <= purposeString.length()) {
            return this;
        } else {
            return cp;
        }
    }

    void addIfBroader(List<Purpose> cps) {
        Purpose existingCompatiblePurpose = null;
        for (Purpose cp: cps) {
            if (isCompatibleWithPurpose(cp) || cp.isCompatibleWithPurpose(this)) {
                existingCompatiblePurpose = cp;
                break;
            }
        }

        if (existingCompatiblePurpose == null) {
            cps.add(this);
        } else if (!existingCompatiblePurpose.isBroaderThan(this)) {
            cps.remove(existingCompatiblePurpose);
            cps.add(this);
        }

        // else to nothing, a broader purpose is in the list already!

    }



    public String toString() {
        return purposeString;
    }
}
