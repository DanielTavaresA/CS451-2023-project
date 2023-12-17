package cs451.Models;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Proposal implements Serializable {
    private Integer activePropose = 0;
    private Set<Integer> proposedValues = new HashSet<Integer>();

    public Proposal(Integer activePropose, Set<Integer> proposedValues) {
        this.activePropose = activePropose;
        this.proposedValues = proposedValues;
    }

    public Integer getActivePropose() {
        return activePropose;
    }

    public Set<Integer> getProposedValues() {
        return proposedValues;
    }

    @Override
    public String toString() {
        return "Proposal [activePropose=" + activePropose + ", proposedValues=" + proposedValues + "]";
    }

}