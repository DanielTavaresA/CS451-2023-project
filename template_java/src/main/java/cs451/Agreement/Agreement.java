package cs451.Agreement;

import cs451.Models.Proposal;

public interface Agreement {
    public void propose(Proposal proposal);

    public void decide(Proposal values);
}
