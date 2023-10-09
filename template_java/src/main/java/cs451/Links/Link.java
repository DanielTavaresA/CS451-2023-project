package cs451.Links;

/**
 * Class representing a link between two hosts.
 */
public interface Link {

    public abstract void send();

    public abstract void receive();

    public abstract void start();

}