package cs451.Agreement;

import java.util.Set;

public interface Agreement {
    public void propose(Set<Integer> proposal);

    public void decide(Set<Integer> values);
}
