package cs451.Models;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LatticeState implements Serializable {
    private Integer slot;
    private boolean active = false;
    private int ackCount = 0;
    private int nackCount = 0;
    private int activePropose = 0;
    private Set<Integer> proposedValues = ConcurrentHashMap.newKeySet();
    private Set<Integer> acceptedValues = ConcurrentHashMap.newKeySet();
    private Set<HostIP> hasAcked = ConcurrentHashMap.newKeySet();
    private Set<HostIP> hasNacked = ConcurrentHashMap.newKeySet();

    public LatticeState(Integer slot, Set<Integer> proposedValues) {
        this.slot = slot;
        this.proposedValues = proposedValues;
    }

    public Integer getSlot() {
        return slot;
    }

    public boolean isActive() {
        return active;
    }

    public int getAckCount() {
        return ackCount;
    }

    public int getNackCount() {
        return nackCount;
    }

    public int getActivePropose() {
        return activePropose;
    }

    public Set<Integer> getProposedValues() {
        return proposedValues;
    }

    public Set<Integer> getAcceptedValues() {
        return acceptedValues;
    }

    public void setSlot(Integer slot) {
        this.slot = slot;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public void incrAckCount() {
        this.ackCount++;
    }

    public void resetAckCount() {
        this.ackCount = 0;
    }

    public void incrNackCount() {
        this.nackCount++;
    }

    public void resetNackCount() {
        this.nackCount = 0;
    }

    public void incrActivePropose() {
        this.activePropose++;
        resetHasAcked();
        resetHasNacked();
    }

    public void addAck(HostIP host) {
        this.hasAcked.add(host);
    }

    public void resetHasAcked() {
        this.hasAcked.clear();
    }

    public void addNack(HostIP host) {
        this.hasNacked.add(host);
    }

    public void resetHasNacked() {
        this.hasNacked.clear();
    }

    public boolean hasResponded(HostIP host) {
        return this.hasAcked.contains(host) || this.hasNacked.contains(host);
    }

}
