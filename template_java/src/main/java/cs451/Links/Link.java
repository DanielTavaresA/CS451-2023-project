package cs451.Links;

import cs451.Models.Message;

/**
 * Class representing a link between two hosts.
 */
public interface Link {

    public abstract boolean send(Message m);

    public abstract boolean receive();

    public abstract void start();

}