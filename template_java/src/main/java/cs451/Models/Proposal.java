package cs451.Models;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Proposal implements Serializable {
    private Integer activePropose = 0;
    private Set<Integer> proposedValues;
    private Integer slot;

    public Proposal(Integer activePropose, Set<Integer> proposedValues, Integer slot) {
        this.activePropose = activePropose;
        this.proposedValues = ConcurrentHashMap.newKeySet();
        this.proposedValues.addAll(proposedValues);
        this.slot = slot;
    }

    public Integer getActivePropose() {
        return activePropose;
    }

    public Set<Integer> getProposedValues() {
        return proposedValues;
    }

    public Integer getSlot() {
        return slot;
    }

    public void setSlot(Integer slot) {
        this.slot = slot;
    }

    @Override
    public String toString() {
        return "Proposal [activePropose=" + activePropose + ", slot=" + slot + ", proposedValues=" + proposedValues
                + "]";
    }

}