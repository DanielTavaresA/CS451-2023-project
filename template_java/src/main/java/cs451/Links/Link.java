package cs451.Links;

import java.net.DatagramPacket;

import cs451.Models.HostIP;
import cs451.Models.Message;

/**
 * Class representing a link between two hosts.
 */
public interface Link {

    public abstract void send(Message m, HostIP dest);

    public abstract void deliver(DatagramPacket packet);

}